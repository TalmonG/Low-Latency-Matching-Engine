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
#include <chrono>
using namespace std;

auto startTime = chrono::high_resolution_clock::now();

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
map<float, map<time_t, list<Order>>, greater<float>> buyOrdersMap; // bid/buy

uint64_t totalOrders = 0;
uint64_t totalTrades = 0;



void DataSender()
{
	int orderId = 0;
	while (true)
	{
		if (networkBuffer.size() < 10000000)
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
							totalTrades++;

							// cleanup
							if (sellOrdersMap.begin()->second.begin()->second.empty())
							{
								sellOrdersMap.begin()->second.erase(sellOrdersMap.begin()->second.begin());

								// erase empty price map
								if (sellOrdersMap.begin()->second.empty())
								{
									sellOrdersMap.erase(sellOrdersMap.begin());
								}
							}
						}
					}
					else
					{
						// store incoming packet to buyOrdersMap
						buyOrdersMap[incomingPacket.price][incomingPacket.timestamp].push_back(incomingPacket);
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
							totalTrades++;

							// cleanup
							if (buyOrdersMap.begin()->second.begin()->second.empty())
							{
								buyOrdersMap.begin()->second.erase(buyOrdersMap.begin()->second.begin());

								// erase empty price map
								if (buyOrdersMap.begin()->second.empty())
								{
									buyOrdersMap.erase(buyOrdersMap.begin());
								}
							}

						}
					}
					else // store incoming packet to sellOrdersMap
					{
						sellOrdersMap[incomingPacket.price][incomingPacket.timestamp].push_back(incomingPacket);
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
		<< "Trades Executed: " << totalTrades << "\033[K\n"
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
