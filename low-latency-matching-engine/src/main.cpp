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
};

mutex bufferMutex;
queue<Order> networkBuffer;

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
		bool hasData;

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
			// process

		}
	}
}

int main()
{
	cout << "Welcome to my Low Latency Matching Engine. Press 'Enter' to begin simulating incoming orders and handling of orders." << endl;
	cin.get();

	// create thread for reciever
	thread reciever(DataReceiver);
	// create thread for sender
	thread sender(DataSender);

	cout << "======================" << endl;
	cout << "Stats" << endl;
	cout << "======================" << endl;

	cin.get();
	return 0;
}