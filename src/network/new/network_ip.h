#pragma once

//lang::CwC

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>

/*
 * each node identified by its id/index and socket address
 */
class NodeInfo {
public:
	unsigned id;
	sockaddr_in address;
};

/*
 * IP based communciation layer. Node 0 is rendezous server
 */
class NetworkIP {//: public NetworkIfc {
public:
	NodeInfo* nodes_;
	size_t this_node_; 	// node index
	int sock_; 					// socket
	sockaddr_in ip_;		// ip address

	NetworkIP() {
		this_node_ = 0;
	}

	~NetworkIP() { close(sock_);}

	size_t index()  { return this_node_; }

	void server_init(unsigned idx, unsigned port, unsigned num_nodes) {
		this_node_ = idx;
		init_sock_(port);

		nodes_ = new NodeInfo[num_nodes];

		for (size_t i = 0; i < num_nodes; ++i) { nodes_[i].id = 0; }
		nodes_[0].address = ip_;
		nodes_[0].id = 0;
		for (size_t i = 2; i <= num_nodes; ++i) {

			Register* msg = recv_register();

			nodes_[msg->sender()].id = msg->sender();
			nodes_[msg->sender()].address.sin_family = AF_INET;
		  	nodes_[msg->sender()].address.sin_addr = msg->client.sin_addr;
			//inet_aton("127.0.0.1", &(nodes_[msg->sender()].address.sin_addr));
			nodes_[msg->sender()].address.sin_port = htons(msg->port);
		}


		size_t* ports = new size_t[num_nodes];
		const char** addresses = new const char*[num_nodes];
		ports[0] = ntohs(ip_.sin_port);
		addresses[0] = inet_ntoa(ip_.sin_addr);
		for (size_t i = 0; i < num_nodes - 1; ++i) {
			ports[i+1] = ntohs(nodes_[i + 1].address.sin_port);
			addresses[i+1] = inet_ntoa(nodes_[i + 1].address.sin_addr);
		}


		for (size_t i = 1; i < num_nodes; ++i) {
			Directory ipd(num_nodes, ports, addresses);

			ipd.dest = i;

			send_m(&ipd);

		}
	}

	void client_init(unsigned idx, unsigned port, char* server_adr, unsigned server_port, unsigned num_nodes) {

		this_node_ = idx;
		init_sock_(port);
		nodes_ = new NodeInfo[1];
		nodes_[0].id = 0;
		nodes_[0].address.sin_family = AF_INET;
		nodes_[0].address.sin_port = htons(server_port);
		if(inet_pton(AF_INET, server_adr, &nodes_[0].address.sin_addr) <= 0) {
			assert(false);
		}


		Register msg(idx, port);

		send_m(&msg);

		Directory* ipd = recv_directory();
		printf("directory receieved\n");


		NodeInfo* nodes = new NodeInfo[num_nodes];
		nodes[0] = nodes_[0];
		for (size_t i = 0; i < ipd->size_dir - 1; i++) {
			nodes[i+1].id = i + 1;
			nodes[i+1].address.sin_family = AF_INET;
			nodes[i+1].address.sin_port = htons(ipd->ports[i]);
			printf("before inet_pton\n");
			if (inet_pton(AF_INET, ipd->addresses[i], &nodes[i+1].address.sin_addr) <= 0) {
				assert(false);
			}
			nodes_ = nodes;
		}
	}

	void init_sock_(unsigned port) {
		assert((sock_ = socket(AF_INET, SOCK_STREAM, 0)) >= 0);
		int opt = 1;
		assert(setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == 0);
		ip_.sin_family = AF_INET;
		ip_.sin_addr.s_addr = INADDR_ANY;
		ip_.sin_port = htons(port);

		assert(bind(sock_, (sockaddr*)&ip_, sizeof(ip_)) >= 0);
		assert(listen(sock_, 100) >= 0);
	}



	void send_message(Message* msg) {
		NodeInfo & tgt = nodes_[msg->target()];

		inet_aton("127.0.0.1", &tgt.address.sin_addr);

		int conn = socket(AF_INET, SOCK_STREAM, 0);
		assert(conn >= 0 && "UNABLE TO CREATE CLIENT SOCKET");
		if (connect(conn, (sockaddr*)&tgt.address, sizeof(struct sockaddr)) < 0) {
			assert(false);
		}

		const char* buf = msg->serialize();
		size_t size = msg->sizeof_serialized();

		send(conn, &size, sizeof(size_t), 0);
		send(conn, buf, size, 0);
	}

	void send_m(Directory* msg)  {

		NodeInfo & tgt = nodes_[msg->target()];
		inet_aton("127.0.0.1", &tgt.address.sin_addr);

		int conn = socket(AF_INET, SOCK_STREAM, 0);
		assert(conn >= 0 && "UNABLE TO CREATE CLIENT SOCKET");

		if (connect(conn, (sockaddr*)&tgt.address, sizeof(tgt.address)) < 0) {
			assert(false);
		}

		const char* buf = msg->serialize();
		size_t size = msg->sizeof_serialized();
		
		send(conn, &size, sizeof(size_t), 0);
		send(conn, buf, size, 0);
	}

	void send_m(Register* msg)  {

		NodeInfo & tgt = nodes_[msg->target()];

		int conn = socket(AF_INET, SOCK_STREAM, 0);
		assert(conn >= 0 && "UNABLE TO CREATE CLIENT SOCKET");
		if (connect(conn, (sockaddr*)&tgt.address, sizeof(struct sockaddr)) < 0) {
			assert(false);
		}

		const char* buf = msg->serialize();
		size_t size = msg->sizeof_serialized() + 1000;

		send(conn, &size, sizeof(size_t), 0);

		send(conn, buf, size, 0);

	}

	Message* recv_m()  {
		sockaddr_in sender;
		socklen_t addrlen = sizeof(sender);
		int req = accept(sock_, (sockaddr*)&sender, &addrlen);
		size_t size = 0;
		if(read(req, &size, sizeof(size_t)) == 0) {
			assert(false);
		}
		char* buf = new char[size];
		int rd = 0;
		while (rd != size) {
			rd += read(req, buf + rd, size - rd);
		}
		Message* msg = Message::deserialize(buf);

		return msg;
	}


	Register* recv_register()  {
		sockaddr_in sender;
		socklen_t addrlen = sizeof(sender);
		int req = accept(sock_, (sockaddr*)&sender, &addrlen);
		size_t size = 0;
		if(read(req, &size, sizeof(size_t)) == 0) {
			assert(false);
		}
		char* buf = new char[size];
		int rd = 0;
		while (rd != size) {
			rd += read(req, buf + rd, size - rd);
		}
		Register* msg = Register::deserialize(buf);

		return msg;
	}

	Directory* recv_directory()  {

		sockaddr_in sender;
		socklen_t addrlen = sizeof(sender);
		int req = accept(sock_, (sockaddr*)&sender, &addrlen);
		size_t size = 0;

		if(read(req, &size, sizeof(size_t)) == 0) {
			assert(false);
		}
		char* buf = new char[size];
		int rd = 0;
		while (rd != size) {

			rd += read(req, buf + rd, size - rd);
		}
		Directory* msg = Directory::deserialize(buf);

		return msg;
	}
};
