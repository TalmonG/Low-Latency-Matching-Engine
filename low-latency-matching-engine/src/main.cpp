#include <iostream>
#include <fstream>
#include <thread>
//#include <ctime>
#include <cstdlib>
#include <vector>
#include <string>
#include <sstream>
#include <queue>
#include <mutex>
#include <map>
#include <list>
#include <chrono>
using namespace std;

auto startTime = chrono::high_resolution_clock::now();

enum class Side : uint8_t {
	NONE,
	BUY,
	SELL
};

enum class Type : uint8_t {
	NONE,
	LIMIT,
	MARKET
};

struct Order
{
public:
	uint64_t orderId = 0;
	uint64_t timestamp = 0;
	enum Side side = Side::NONE;
	enum Type type = Type::NONE;
	uint32_t price = 0;
	uint64_t quantity = 0;

	bool operator==(const Order& other) const
	{
		return orderId == other.orderId; // compare orders by Id
	}
};

mutex bufferMutex;
queue<Order> networkBuffer;
map<uint64_t, list<Order>> sellOrdersMap; // ask/sell
map<uint64_t, list<Order>, greater<uint64_t>> buyOrdersMap; // bid/buy

uint64_t totalOrders = 0;
uint64_t totalTrades = 0;



void DataSender()
{
	int orderId = 1;
	while (true)
	{
		if (networkBuffer.size() < 10000000)
		{
			// send at random
			uint64_t timestamp = chrono::duration_cast<chrono::nanoseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
			Side side = (rand() % 2 == 0) ? Side::BUY : Side::SELL;
			Type type = (rand() % 2 == 0) ? Type::LIMIT : Type::MARKET;
			uint64_t price = round(24800 + (uint64_t(rand()) / RAND_MAX) * (25100 - 24800));
			uint64_t quantity = (rand() % 999) + 1; // 1 to 1000

			// create packet
			Order newOrder = { orderId++, timestamp, side, type, price, quantity };

			// send packet
			{
				lock_guard<mutex> lock(bufferMutex);
				networkBuffer.push(newOrder);
			}
		}
	}
}

void DataReceiver()
{
	while (true)
	{
		Order incomingPacket;
		bool hasData = false;

		{
			lock_guard<mutex> lock(bufferMutex);
			if (!networkBuffer.empty())
			{
				incomingPacket = networkBuffer.front();
				networkBuffer.pop();
				hasData = true;
				totalOrders++;
			}
		}

		if (hasData)
		{
			if (incomingPacket.orderId > 0 && incomingPacket.price > 0
				&& incomingPacket.quantity > 0 && incomingPacket.timestamp > 0
				&& incomingPacket.side != Side::NONE
				&& incomingPacket.type != Type::NONE)
			{
				// process
				if (incomingPacket.side == Side::BUY) // they want to buy
				{
					if (incomingPacket.type == Type::LIMIT)
					{
						if (!sellOrdersMap.empty()) // is there a seller
						{
							if (incomingPacket.price >= sellOrdersMap.begin()->second.front().price) // priceMap->ordersList
							{
								// lowest price at oldest order
								sellOrdersMap.begin()->second.pop_front(); // TODO: should lock on this
								totalTrades++;

								// cleanup
								if (sellOrdersMap.begin()->second.empty())
								{
									sellOrdersMap.erase(sellOrdersMap.begin());
								}
							}
							else
							{
								// store incoming packet to buyOrdersMap
								buyOrdersMap[incomingPacket.price].push_back(incomingPacket);
							}
						}
						else
						{
							// store incoming packet to buyOrdersMap
							buyOrdersMap[incomingPacket.price].push_back(incomingPacket);
						}
					}
					else // market type
					{
						if (!sellOrdersMap.empty()) // is there a seller
						{
							// lowest price at oldest order
							sellOrdersMap.begin()->second.pop_front(); // TODO: should lock on this
							totalTrades++;

							// cleanup
							if (sellOrdersMap.begin()->second.empty())
							{
								sellOrdersMap.erase(sellOrdersMap.begin());
							}
						}
						else
						{
							// cancel order
						}
					}
				}
				else if (incomingPacket.side == Side::SELL) // they want to sell
				{
					if (incomingPacket.type == Type::LIMIT)
					{
						if (!buyOrdersMap.empty())
						{
							// compare against highest bid/buy
							//				               lowest price	       orders list
							if (incomingPacket.price <= buyOrdersMap.begin()->second.front().price) // priceMap->ordersList
							{
								// lowest price at oldest order
								buyOrdersMap.begin()->second.pop_front();
								totalTrades++;

								// cleanup
								if (buyOrdersMap.begin()->second.empty())
								{
									buyOrdersMap.erase(buyOrdersMap.begin());
								}
							}
							else
							{
								// store incoming packet to buyOrdersMap
								sellOrdersMap[incomingPacket.price].push_back(incomingPacket);
							}
						}
						else
						{
							// store incoming packet to buyOrdersMap
							sellOrdersMap[incomingPacket.price].push_back(incomingPacket);
						}
					}
					else // market type
					{
						if (!buyOrdersMap.empty())
						{
							// lowest price at oldest order
							buyOrdersMap.begin()->second.pop_front();
							totalTrades++;

							// cleanup
							if (buyOrdersMap.begin()->second.empty())
							{
								buyOrdersMap.erase(buyOrdersMap.begin());
							}
						}
						else
						{
							// cancel order
						}
					}
				}
			}
		}
	}
}

void Display()
{
	auto currentTime = chrono::high_resolution_clock::now();
	double seconds = chrono::duration<double>(currentTime - startTime).count();
	int throughput = (seconds > 0) ? (totalOrders / seconds) : 0;

	size_t currentQueueSize = 0;
	{
		lock_guard<mutex> lock(bufferMutex);
		currentQueueSize = networkBuffer.size();
	}

	stringstream ss;
	ss << "\033[H"
		<< "======================\033[K\n"
		<< "Orders Processed: " << totalOrders << "\033[K\n"
		<< "Trades Executed:  " << totalTrades << "\033[K\n"
		<< "Current Throughput: " << throughput << " orders/sec\033[K\n"
		<< "Average Latency: coming soon ns\033[K\n"
		<< "Total Queued Orders: " << currentQueueSize << "\033[K\n"
		<< "======================\033[K\n"
		<< "Press 'CTRL + C' To Stop\033[K\n";
	cout << ss.str() << flush;
}

int main()
{
	cout << "\033[?25l";
	cout << "Welcome to my Low Latency Matching Engine. Press 'Enter' to begin simulating incoming orders and handling of orders." << endl;
	cin.get();

	// create thread for reciever
	thread reciever(DataReceiver);
	// create thread for sender
	thread sender(DataSender);


	while (true)
	{
		Display();
		this_thread::sleep_for(chrono::seconds(1));
	}
	return 0;
}
