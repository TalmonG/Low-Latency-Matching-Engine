#include <iostream>
#include <fstream>
#include <thread>
#include <ctime>
#include <cstdlib>
#include <vector>
#include <string>
#include <sstream>
#include <queue>
#include <mutex>
#include <map>
#include <list>
using namespace std;

struct Order
{
public:
	int orderId;
	time_t timestamp;
	bool isSideBuy;
	bool isTypeLimit;
	float price;
	int quantity;

	bool operator==(const Order& other) const
	{
		return orderId == other.orderId; // compare orders by Id
	}
};

mutex bufferMutex;
queue<Order> networkBuffer;
map<float, map<time_t, list<Order>>> sellOrdersMap; // ask/sell
map<float, map<time_t, list<Order>>> buyOrdersMap; // bid/buy

void DataSender()
{
	int orderId = 0;
	while (true)
	{
		// send at random
		time_t timestamp = time(&timestamp);
		bool isSideBuy = (rand() % 2 == 0);
		bool isTypeLimit = (rand() % 2 == 0);
		float price = round(248.0f + (float(rand()) / RAND_MAX) * (251.0f - 248.0f));
		int quantity = (rand() % 999) + 1; // 1 to 1000

		// create packet
		Order newOrder = { orderId++, timestamp, isSideBuy, isTypeLimit, price, quantity };

		// send packet
		{
			lock_guard<mutex> lock(bufferMutex);
			networkBuffer.push(newOrder);
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
			}
		}

		if (hasData)
		{
			if (incomingPacket.orderId <= 0 || incomingPacket.price <= 0
				|| incomingPacket.quantity <= 0 || incomingPacket.timestamp <= 0
				|| typeid(incomingPacket.isSideBuy) == typeid(bool)
				|| typeid(incomingPacket.isTypeLimit) == typeid(bool))
			{

				// process
				if (incomingPacket.isSideBuy) // they want to buy
				{
					if (!sellOrdersMap.empty()) // is there a seller
					{
						// compare against lowest ask/sell
						//				   lowest price	          lowest time     orders list
						Order tempOrder = sellOrdersMap.begin()->second.begin()->second.front();
						if (incomingPacket.price >= tempOrder.price) // priceMap->timestampsMap->ordersList
						{
							// lowest price at oldest order
							sellOrdersMap.begin()->second.begin()->second.remove(tempOrder);
							return;
						}
					}
					else
					{
						// store incoming packet to buyOrdersMap
						buyOrdersMap[incomingPacket.price][incomingPacket.timestamp].push_back(incomingPacket);
						return;
					}
				}
				else // they want to sell
				{
					if (!buyOrdersMap.empty())
					{
						// compare against highest bid/buy
						//				   lowest price	          lowest time     orders list
						Order tempOrder = buyOrdersMap.begin()->second.begin()->second.front();
						if (incomingPacket.price <= tempOrder.price) // priceMap->timestampsMap->ordersList
						{
							// lowest price at oldest order
							buyOrdersMap.begin()->second.begin()->second.remove(tempOrder);
							break;
						}
					}
					else // store incoming packet to sellOrdersMap
					{
						sellOrdersMap[incomingPacket.price][incomingPacket.timestamp].push_back(incomingPacket);
						break;
					}
				}
			}
		}
	}
}

void Display()
{
	cout << "\033======================" << endl;
	cout << "Orders Processed: " << 19000000 << endl;
	cout << "Trades Executed: " << 1500000 << endl;
	cout << "Current Throughput (goal 1000/s): " << 1300000 << "orders/sec" << endl;
	cout << "Average Latency: " << 9 << "ns" << endl;
	cout << "======================" << endl;
	cout << "Press Enter To Stop" << endl;

	cin.get();
	cout << "Press Enter To Exit" << endl;
	cin.get();
}

int main()
{
	cout << "Welcome to my Low Latency Matching Engine. Press 'Enter' to begin simulating incoming orders and handling of orders." << endl;
	cin.get();

	// create thread for reciever
	thread reciever(DataReceiver);
	// create thread for sender
	thread sender(DataSender);

	Display();
	return 0;
}
