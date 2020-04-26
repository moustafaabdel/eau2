#pragma once

/*
 * Abstract communication layer between nodes.
 * Heavy inspiration from Professor Vitek
 */
class NetworkIfc {
public:
	virtual void register_node(size_t idx) { }

	//return node index
	virtual size_t index() { assert(false); }

	// send message and delete message
	virtual void send_m(Message* msg) = 0;

	// wait for message to arrive and take ownership
	virtual Message* recv_m() = 0;

};
