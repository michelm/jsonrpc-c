/*
 * jsonrpc-c.c
 *
 *  Created on: Oct 11, 2012
 *      Author: hmng
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>

#include "jsonrpc-c.h"

#define EV_STANDALONE	1
#define	EV_USE_SELECT	1
#define EV_USE_EPOLL	0
#define	EV_USE_POLL		0

#include "ev.h"
//#include "ev.c" -->> use libev

struct jrpc_connection {
	struct ev_io io;	// must be the first structure in jrpc_connection (sub-classed)
	int fd;
	int pos;
	unsigned int buffer_size;
	char * buffer;
	struct jrpc_server *server;
	char * url;
	UT_hash_handle hh;		// hash on url
};

static int __jrpc_server_start(struct jrpc_server *server);
static void jrpc_procedure_destroy(struct jrpc_procedure *procedure);

struct ev_loop *loop;

// get sockaddr, IPv4 or IPv6:
static void *get_in_addr(struct sockaddr *sa) {
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*) sa)->sin_addr);
	}
	return &(((struct sockaddr_in6*) sa)->sin6_addr);
}

static int get_port(struct sockaddr *sa) {
	if (sa->sa_family == AF_INET) {
		return ntohs(((struct sockaddr_in*) sa)->sin_port);
	}
	return ntohs(((struct sockaddr_in6*) sa)->sin6_port);
}

static int send_response(struct jrpc_connection * conn, char *response) {
	int result = 0;
	int fd = conn->fd;
	if (conn->server->debug_level > 1)
		printf("JSON Response (%s):\n%s\n", conn->url, response);
	result = send(fd, response, strlen(response), MSG_DONTWAIT);
	if (result >= 0)
		result = send(fd, "\n", 1, MSG_DONTWAIT);
	return result;
}

/*
 * Send a json-rpc error response.
 * This is done based on the json-rpc 2.0 specification given at:
 * http://www.jsonrpc.org/specification
 *
 * TODO: add support <error> member: <data>.
 */
static int send_error(struct jrpc_connection * conn, int code, char* message,
		cJSON * id) {
	int return_value = 0;

	/*
	 * the response object has the following members:
	 * <jsonrpc>:	MANDATORY, MUST be exactly "2.0" (i.e. the supported JRPC_VERSION)
	 * <error>	:	MANDATORY, with the following members:
	 * 				<code>	 : MANDATORY, a Number that indicates the error type that occurred.
	 * 				<message>: short description of the error (single sentence)
	 * 				<data>	 : NOT SUPPORTED YET (may be omitted)
	 * 						   TODO: add support of <data>
	 * <id>		: 	MANDATORY, MUST be the same as the value of the id member in the Request Object.
	 * 				If there was an error in detecting the id in the Request object (e.g. Parse error/Invalid Request), it MUST be Null.
	 */
	cJSON *result_root = cJSON_CreateObject();
	cJSON *error_root = cJSON_CreateObject();
	cJSON_AddItemToObject(result_root, "jsonrpc", cJSON_CreateString(JRPC_VERSION));
	cJSON_AddNumberToObject(error_root, "code", code);
	cJSON_AddStringToObject(error_root, "message", message);
	cJSON_AddItemToObject(result_root, "error", error_root);
	cJSON_AddItemToObject(result_root, "id", (id == NULL)? cJSON_CreateString("null"): id);

	char * str_result;
	if (conn->server->formatted)
		str_result = cJSON_Print(result_root);
	else
		str_result = cJSON_PrintUnformatted(result_root);
	return_value = send_response(conn, str_result);
	free(str_result);
	cJSON_Delete(result_root);
	free(message);
	return return_value;
}

/*
 * Send a json-rpc successful response.
 * This is done based on the json-rpc 2.0 specification given at:
 * http://www.jsonrpc.org/specification
 */
static int send_result(struct jrpc_connection * conn, cJSON * result,
		cJSON * id) {
	int return_value = 0;

	/*
	 * the response object has the following members:
	 * <jsonrpc>:	MANDATORY, MUST be exactly "2.0" (i.e. the supported JRPC_VERSION)
	 * <result>	:	MANDATORY, the value of this member is determined by the method invoked in the request.
	 * <id>		: 	MANDATORY, MUST be the same as the value of the id member in the Request Object.
	 * 				If there was an error in detecting the id in the Request object (e.g. Parse error/Invalid Request), it MUST be Null.
	 */
	cJSON *result_root = cJSON_CreateObject();
	cJSON_AddItemToObject(result_root, "jsonrpc", cJSON_CreateString(JRPC_VERSION));
	if (result)
		cJSON_AddItemToObject(result_root, "result", result);
	cJSON_AddItemToObject(result_root, "id", (id == NULL)? cJSON_CreateString("null"): id);
	char * str_result;
	if (conn->server->formatted)
		str_result = cJSON_Print(result_root);
	else
		str_result = cJSON_PrintUnformatted(result_root);
	return_value = send_response(conn, str_result);
	free(str_result);
	cJSON_Delete(result_root);
	return return_value;
}

static int invoke_procedure(struct jrpc_server *server, struct jrpc_connection * conn, char *name, cJSON *params, cJSON *id) {
	cJSON *returned = NULL;
	struct jrpc_procedure *proc;
	int result = 0;

	HASH_FIND_STR(server->procedures, name, proc);
	if (proc == NULL) {
		char error_str[256];
		int n = snprintf(error_str, sizeof(error_str), "method not found: %s", name);
		if (n < 0 || n >= sizeof(error_str)) {
			return send_error(conn, JRPC_METHOD_NOT_FOUND, strdup(name), id);
		} else {
			return send_error(conn, JRPC_METHOD_NOT_FOUND, strdup(error_str), id);
		}
	}

	jrpc_context *ctx = calloc(1, sizeof(jrpc_context));

	ctx->formatted = server->formatted;
	ctx->data = proc->data;
	ctx->url = strdup(conn->url);
	returned = proc->function(ctx, params, id);
	server->formatted = ctx->formatted;

	if (returned == NULL && ctx->error_code == 0) {
		// nothing to do
	} else {
		if (ctx->error_code)
			result = send_error(conn, ctx->error_code, ctx->error_message, id);
		else
			result = send_result(conn, returned, id);
	}
	free(ctx->url);
	free(ctx);

	return result;
}

int jrpc_send_reply(struct jrpc_server *server, jrpc_context *ctx, cJSON* result, cJSON* id) {
	int retval = -1;
	server->formatted = ctx->formatted;

	struct jrpc_connection * conn;
	HASH_FIND_STR(server->connections, ctx->url, conn);
	if (conn == NULL) {
		printf("Connection %s not found\n", ctx->url);
		cJSON_Delete(result);
		cJSON_Delete(id);
		goto exit;
	}

	if (ctx->error_code)
		retval = send_error(conn, ctx->error_code, ctx->error_message, id);
	else
		retval = send_result(conn, result, id);
	exit:
	free(ctx->url);
	free(ctx);
	return retval;
}

jrpc_context *jrpc_context_dup(jrpc_context *context) {
	jrpc_context *result = calloc(1, sizeof(jrpc_context));
	result->data = context->data;
	result->error_code = context->error_code;
	if (context->error_message != NULL)
		result->error_message = strdup(context->error_message);
	else
		result->error_message = NULL;
	result->formatted = context->formatted;
	result->url = strdup(context->url);
	return result;
}

cJSON * getRequestMember(cJSON *root,const char *memberName) {
	cJSON *member = NULL;
	member = cJSON_GetObjectItem(root, memberName);
	return member;
}


/*
 * Evaluate a json-rpc requests, pushing valid requests through.
 * This is done based on the json-rpc 2.0 specification given at:
 * http://www.jsonrpc.org/specification
 */
static int eval_request(struct jrpc_server *server,
		struct jrpc_connection * conn, cJSON *root) {
	cJSON *jsonrpc, *method, *params, *id;

	/* Get all Request object members.
	 * These are: <jsonrpc> , <method, <params> and <id>
	 */
	jsonrpc = getRequestMember(root, "jsonrpc");
	method = getRequestMember(root, "method");
	params = getRequestMember(root, "params");
	id = getRequestMember(root, "id");

	
	/*
	 * Check the JSON-RPC protocol version
	 * If not specified, we assume "1.0".
	 * We fully support version "2.0", we do our best with "1.0".
	 * Version other than "2.0".
	 */
	if (jsonrpc == NULL) {
		/* assuming jsonrpcVersion = 1.0 */
		// jsonrpc = cJSON_CreateString("1.0");
	} else {
		if (strcmp(jsonrpc->valuestring, JRPC_VERSION) != 0) {
			char error_str[256];
			int n = snprintf(error_str, sizeof(error_str), "Unsupported JSON-RPC protocol version: %s", jsonrpc->valuestring);
			if (n < 0 || n >= sizeof(error_str)) {
				return send_error(conn, JRPC_PARSE_ERROR, strdup(jsonrpc->valuestring), id);
			} else {
				return send_error(conn, JRPC_PARSE_ERROR, strdup(error_str), id);
			}
		}
	}

	/*
	 * Process <method, <params> and <id>
	 * <method>	: is MANDATORY
	 * <params>	: MAY be omitted
	 * <id>		: MAY be omitted, if included MUST contain a String, Number, or NULL value.
	 * 			  This member is used to correlate the context between the two objects.
	 * 			  If it is not included it is assumed to be a notification.
	 * 			  The Server MUST reply with the same value in the Response object if included.
	 */
	if (method != NULL && method->type == cJSON_String) {
		if (params == NULL|| params->type == cJSON_Array
				|| params->type == cJSON_Object) {
			if (id == NULL|| id->type == cJSON_String
					|| id->type == cJSON_Number) {
				//We have to copy ID because using it on the reply and deleting the response Object will also delete ID
				cJSON * id_copy = NULL;
				if (id != NULL)
					id_copy =
							(id->type == cJSON_String) ? cJSON_CreateString(
									id->valuestring) :
									cJSON_CreateNumber(id->valueint);
				if (server->debug_level)
					printf("Method Invoked (%s): %s\n", conn->url, method->valuestring);
				return invoke_procedure(server, conn, method->valuestring,
						params, id_copy);
			}
		}
	}
	return send_error(conn, JRPC_INVALID_REQUEST,
			strdup("The JSON sent is not a valid Request object."), id);
}

static void free_connection(struct jrpc_connection *conn) {
	close(conn->fd);
	free(conn->buffer);
	free(conn->url);
	free(conn);
}

static void close_connection(struct ev_loop *loop, ev_io *w) {
	ev_io_stop(loop, w);
	struct jrpc_connection *conn = (struct jrpc_connection *) w;
	HASH_DEL(conn->server->connections, conn);
	free_connection(conn);
}

static void connection_cb(struct ev_loop *loop, ev_io *w, int revents) {
	struct jrpc_connection *conn;
	struct jrpc_server *server = (struct jrpc_server *) w->data;
	size_t bytes_read = 0;
	//get our 'subclassed' event watcher
	conn = (struct jrpc_connection *) w;
	int fd = conn->fd;
	if (conn->pos == (conn->buffer_size - 1)) {
		char * new_buffer = realloc(conn->buffer, conn->buffer_size *= 2);
		if (new_buffer == NULL) {
			perror("Memory error");
			return close_connection(loop, w);
		}
		conn->buffer = new_buffer;
		memset(conn->buffer + conn->pos, 0, conn->buffer_size - conn->pos);
	}
	// can not fill the entire buffer, string must be NULL terminated
	int max_read_size = conn->buffer_size - conn->pos - 1;
	if ((bytes_read = read(fd, conn->buffer + conn->pos, max_read_size))
			== -1) {
		perror("read");
		return close_connection(loop, w);
	}
	if (!bytes_read) {
		// client closed the sending half of the connection
		if (server->debug_level)
			printf("Client closed connection (%s).\n", conn->url);
		return close_connection(loop, w);
	} else {
		cJSON *root;
		char *end_ptr;
		int mustParse= 1;
		conn->pos += bytes_read;

		while (mustParse) {
			if ((root = cJSON_Parse_Stream(conn->buffer, &end_ptr)) != NULL) {
				if (server->debug_level > 1) {
					char * str_result = cJSON_Print(root);
					printf("Valid JSON Received (%s):\n%s\n", conn->url, str_result);
					free(str_result);
				}

				int result = 0;
				if (root->type == cJSON_Object) {
					result = eval_request(server, conn, root);
				}
				cJSON_Delete(root);
				//shift after processed request
				memmove(conn->buffer, end_ptr, strlen(end_ptr)+1);

				conn->pos = strlen(end_ptr);
				if (strlen(end_ptr) <= 0){
					mustParse = 0;
				}

				if (result < 0) {
					fprintf(stderr, "+++ jsonrpc-c.c: closing on blocking write\n");
					return close_connection(loop, w);
				}
			} else {
				// did we parse the all buffer? If so, just wait for more.
				// else there was an error before the buffer's end
				if (cJSON_GetErrorPtr() != (conn->buffer + conn->pos)) {
					if (server->debug_level) {
						printf("INVALID JSON Received (%s):\n---\n%s\n---\n", conn->url, conn->buffer);
					}
					send_error(conn, JRPC_PARSE_ERROR,
							strdup(
									"Parse error. Invalid JSON was received by the server."),
									NULL);
					return close_connection(loop, w);
				}
				mustParse = 0;
			}
		}
		conn->pos = 0;
		memset(conn->buffer, 0, conn->buffer_size);

	}

}

static void accept_cb(struct ev_loop *loop, ev_io *w, int revents) {
	char s[INET6_ADDRSTRLEN];
	int port;
	struct jrpc_connection *connection_watcher;
	connection_watcher = malloc(sizeof(struct jrpc_connection));
	struct sockaddr_storage their_addr; // connector's address information
	socklen_t sin_size;
	sin_size = sizeof their_addr;
	connection_watcher->fd = accept(w->fd, (struct sockaddr *) &their_addr,
			&sin_size);
	if (connection_watcher->fd == -1) {
		perror("accept");
		free(connection_watcher);
	} else {
		connection_watcher->url = malloc(INET6_ADDRSTRLEN + 6);
		inet_ntop(their_addr.ss_family,	get_in_addr((struct sockaddr *) &their_addr), s, sizeof s);
		port = get_port((struct sockaddr *) &their_addr);
		sprintf(connection_watcher->url, "%s:%d", s, port);
		if (((struct jrpc_server *) w->data)->debug_level) {
			printf("server: got connection from %s\n", connection_watcher->url);
		}

		connection_watcher->buffer_size = 1500;
		connection_watcher->buffer = malloc(1500);
		memset(connection_watcher->buffer, 0, 1500);
		connection_watcher->pos = 0;
		connection_watcher->server = w->data;

		HASH_ADD_KEYPTR(hh, connection_watcher->server->connections, connection_watcher->url, strlen(connection_watcher->url), connection_watcher);

		ev_io_init(&connection_watcher->io, connection_cb, connection_watcher->fd, EV_READ);
		connection_watcher->io.data = connection_watcher->server;
		ev_io_start(loop, &connection_watcher->io);
	}
}

struct jrpc_server *jrpc_create_server() {
	return calloc(1, sizeof(struct jrpc_server));
}

int jrpc_server_init(struct jrpc_server *server, int port_number) {
	loop = EV_DEFAULT;
	return jrpc_server_init_with_ev_loop(server, port_number, loop);
}

int jrpc_server_init_with_ev_loop(struct jrpc_server *server,
		int port_number, struct ev_loop *loop) {
	memset(server, 0, sizeof(struct jrpc_server));
	server->loop = loop;
	server->port_number = port_number;
	char * debug_level_env = getenv("JRPC_DEBUG");
	if (debug_level_env == NULL)
		server->debug_level = 0;
	else {
		server->debug_level = strtol(debug_level_env, NULL, 10);
		printf("JSONRPC-C Debug level %d\n", server->debug_level);
	}
	return __jrpc_server_start(server);
}

static int __jrpc_server_start(struct jrpc_server *server) {
	int sockfd;
	struct addrinfo hints, *servinfo, *p;
	int yes = 1;
	int rv;
	char PORT[6];
	sprintf(PORT, "%d", server->port_number);
	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE; // use my IP

	if ((rv = getaddrinfo(NULL, PORT, &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return 1;
	}

	// loop through all the results and bind to the first we can
	for (p = servinfo; p != NULL; p = p->ai_next) {
		fprintf(stderr, "Creating socket fam=%d, type=%d, prot=%d\n",
				p->ai_family, p->ai_socktype, p->ai_protocol);
		if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol))
				== -1) {
			perror("server: socket");
			continue;
		}

		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int))
				== -1) {
			perror("setsockopt");
			exit(1);
		}

		if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
			close(sockfd);
			perror("server: bind");
			continue;
		}

		break;
	}

	if (p == NULL) {
		fprintf(stderr, "server: failed to bind\n");
		return 2;
	}

	freeaddrinfo(servinfo); // all done with this structure

	if (listen(sockfd, 5) == -1) {
		perror("listen");
		exit(1);
	}
	if (server->debug_level)
		printf("server: waiting for connections...\n");

	ev_io_init(&server->listen_watcher, accept_cb, sockfd, EV_READ);
	server->listen_watcher.data = server;
	ev_io_start(server->loop, &server->listen_watcher);
	return 0;
}

void jrpc_server_run(struct jrpc_server *server){
	ev_run(server->loop, 0);
}

int jrpc_server_stop(struct jrpc_server *server) {
	ev_break(server->loop, EVBREAK_ALL);
	return 0;
}

void jrpc_server_destroy(struct jrpc_server *server){
	struct jrpc_procedure *proc, *tmp;
	HASH_ITER(hh, server->procedures, proc, tmp) {
		HASH_DEL(server->procedures, proc);
		jrpc_procedure_destroy(proc);
	}
	free(server);
}

static void jrpc_procedure_destroy(struct jrpc_procedure *procedure){
	if (procedure->name){
		free(procedure->name);
		procedure->name = NULL;
	}
	if (procedure->data){
		free(procedure->data);
		procedure->data = NULL;
	}
	free(procedure);
}

int jrpc_register_procedure(struct jrpc_server *server,	jrpc_function function_pointer, char *name, void * data) {
	struct jrpc_procedure *proc = malloc(sizeof(struct jrpc_procedure));
	proc->data = data;
	proc->function = function_pointer;
	proc->name = strdup(name);
	HASH_ADD_KEYPTR(hh, server->procedures, proc->name, strlen(proc->name), proc);
	//	struct jrpc_procedure *tmp;
	//	HASH_REPLACE(hh, server->procedures, name, strlen(proc->name), proc, tmp);
	//	if (tmp != NULL)
	//		jrpc_procedure_destroy(tmp);
	return 0;
}

int jrpc_deregister_procedure(struct jrpc_server *server, char *name) {
	/* Search the procedure to deregister */
	struct jrpc_procedure *proc;
	HASH_FIND_STR(server->procedures, name, proc);
	if (proc == NULL) {
		fprintf(stderr, "server : procedure '%s' not found\n", name);
		return -1;
	}
	HASH_DEL(server->procedures, proc);
	jrpc_procedure_destroy(proc);
	return 0;
}

int jrpc_set_formatted(struct jrpc_server *server, int fmt) {
	server->formatted = fmt;
	return 0;
}
