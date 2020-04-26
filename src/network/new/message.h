#pragma once
#include "../../util/serializer.h"
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstdlib>

class Message {
public:
	char type;
	unsigned src;
	unsigned dest;
	const char* payload;
	size_t payload_size;

	Message(unsigned sender, unsigned receiver, char t, const char* serialized_data, size_t size_of_serialized) {
		type = t;
		src = sender;
		dest = receiver;
		payload = serialized_data;
		payload_size = size_of_serialized;
	}

	Message() {
		type = 'M';
		src = 0;
		dest = 0;
		payload = " ";
		payload_size = 0;
	}

	virtual ~Message() {}

 	const char* serialize() {
		Serialize* serial = new Serialize();
		serial->serialize(type);
		serial->serialize(src);
		serial->serialize(dest);
		serial->serialize(payload_size);
		serial->serialize(payload_size,payload);
		return serial->buffer;
	}

	size_t sizeof_serialized() {
		return (2 * sizeof(unsigned)) + sizeof(char)
		+ sizeof(size_t) + payload_size * sizeof(char) * 2;
	}

	virtual unsigned target() { return dest; }

	static Message* deserialize(const char* msg) {
		size_t buff_size = 0;

		char type = Deserialize::deserialize_char(msg);
		buff_size += sizeof(type);

		unsigned src = Deserialize::deserialize_unsigned(msg + buff_size);
		buff_size += sizeof(src);

		unsigned dest = Deserialize::deserialize_unsigned(msg + buff_size);
		buff_size += sizeof(dest);

		size_t p_size = Deserialize::deserialize_size_t(msg + buff_size);
		buff_size += sizeof(p_size);

		char* payload = Deserialize::deserialize(msg + buff_size, p_size);
		buff_size += sizeof(payload);

		Message* message = new Message(src, dest, type, payload, p_size);

		return message;

	}
};

class Register : public Message {
public:
		char type;
    sockaddr_in client;
    size_t port;

		Register(unsigned idx, unsigned p) {
			type = 'R';
			struct sockaddr_in cl;
			cl.sin_addr.s_addr = idx;
			client = cl;
			port = p;
			src = idx;
			dest = 0;
		}



		virtual size_t sizeof_serialized() {
  		return sizeof(unsigned) + (sizeof(size_t));
  	}

		unsigned sender() {
			return client.sin_addr.s_addr;
		}



		const char* serialize() {

			Serialize* serial = new Serialize();

			serial->serialize(port);
			serial->serialize(client.sin_addr.s_addr);

			return serial->buffer;

		}


		static Register* deserialize(const char* msg) {

			size_t buff_size = 0;

			size_t port = Deserialize::deserialize_size_t(msg + buff_size);
			buff_size += sizeof(size_t);

			unsigned idx = Deserialize::deserialize_unsigned(msg + buff_size);
			buff_size += sizeof(unsigned);


			Register* reg = new Register(idx, port);

			return reg;
		}
};



class Directory : public Message {
public:
	 char type;
	 size_t size_dir;
   size_t * ports;  // owned
   const char ** addresses;  // owned; strings owned


	 Directory(size_t dirsz, size_t* p, const char** addrs) {
		 type = 'D';
		 size_dir = dirsz;
		 ports = p;
		 addresses = addrs;

		 src = 0;
		 dest = 0;
	 }

	 virtual size_t sizeof_serialized() {
 		return sizeof(size_t) + size_dir * ((sizeof(size_t) + 7));
 	}

	  const char* serialize() {

		 Serialize* serial = new Serialize();
		 serial->serialize(size_dir);

		 for (size_t i = 0; i < size_dir; i++) {
			 serial->serialize(ports[i]);
		 }
		 for (size_t i = 0; i < size_dir; i++) {
			 // printf("size of address: %zu\n", strlen(addresses[i]));
			 // printf("address: %s\n", addresses[i]);

			 serial->serialize(7, addresses[i]);
		 }

		 return serial->buffer;

	 }

	 static Directory* deserialize(const char* message) {
		 size_t buff_size = 0;

		 size_t size_dir = Deserialize::deserialize_size_t(message + buff_size);
		 buff_size = sizeof(size_t);

		 size_t* ports = new size_t[size_dir];
		 for (size_t i = 0; i < size_dir; i++) {
			 ports[i] = Deserialize::deserialize_size_t(message + buff_size);
			 buff_size += sizeof(size_t);
		 }

		 const char** addresses = new const char*[size_dir];
		 for (size_t i = 0; i < size_dir; i++) {
			 addresses[i] = "127.0.0.1";
			 buff_size += 7;
		 }

		 Directory* dir = new Directory(size_dir, ports, addresses);

		 return dir;
	 }

};
