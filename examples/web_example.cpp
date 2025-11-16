// examples/web_example.cpp
#include "ConnectionPool.h"
#include "AsyncExecutor.h"
#include "QueryBuilder.h"
#include "ValueVisitor.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <vector>

// Simulated web request handler
void handleRequest(int requestId, sqlite_flux::ConnectionPool& pool)
{
	try
	{
		std::cout << "[Request " << requestId << "] Starting\n";

		// Acquire connection from pool (RAII - auto-released)
		auto conn = pool.acquire();

		std::cout << "[Request " << requestId << "] Got connection\n";

		// Build and execute query
		sqlite_flux::QueryFactory factory(*conn);
		auto results = factory.FromTable("users")
			.Columns("id", "username", "email")
			.Filter("is_active", int64_t(1))
			.Limit(5)
			.Execute();

		std::cout << "[Request " << requestId << "] Found " << results.size() << " users\n";

		// Simulate processing time
		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		std::cout << "[Request " << requestId << "] Completed\n";

		// Connection auto-released here when conn goes out of scope
	}
	catch (const std::exception& e)
	{
		std::cerr << "[Request " << requestId << "] Error: " << e.what() << "\n";
	}
} // end of handleRequest

// Simulated async web request handler
sqlite_flux::Task<void> handleAsyncRequest(int requestId, sqlite_flux::AsyncExecutor& executor)
{
	try
	{
		std::cout << "[Async Request " << requestId << "] Starting\n";

		// Async query
		auto results = co_await executor.query(
			"SELECT id, username, email FROM users WHERE is_active = 1 LIMIT 5"
		);

		std::cout << "[Async Request " << requestId << "] Found " << results.size() << " users\n";

		// Simulate processing
		std::this_thread::sleep_for(std::chrono::milliseconds(100));

		std::cout << "[Async Request " << requestId << "] Completed\n";
	}
	catch (const std::exception& e)
	{
		std::cerr << "[Async Request " << requestId << "] Error: " << e.what() << "\n";
	}
} // end of handleAsyncRequest

int main()
{
	try
	{
		std::cout << "=== sqlite_flux Web Application Example ===\n\n";

		// Find database path
		std::string dbPath = "../../../databases/testdb.db";  // From build/bin/Release

		// ====================================================================
		// Example 1: Connection Pool with Synchronous API
		// ====================================================================
		std::cout << "=== Example 1: Connection Pool (Sync) ===\n";
		{
			// Create connection pool with 5 connections
			sqlite_flux::ConnectionPool pool(dbPath, 5, true);

			std::cout << "Connection pool created:\n";
			std::cout << "  Total connections: " << pool.size() << "\n";
			std::cout << "  Available: " << pool.available() << "\n\n";

			// Simulate 10 concurrent web requests
			std::vector<std::thread> threads;

			auto startTime = std::chrono::steady_clock::now();

			for (int i = 0; i < 10; ++i)
			{
				threads.emplace_back(handleRequest, i + 1, std::ref(pool));
			} // end of for

			// Wait for all requests to complete
			for (auto& thread : threads)
			{
				thread.join();
			} // end of for

			auto endTime = std::chrono::steady_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

			std::cout << "\nAll requests completed in " << duration.count() << "ms\n";
			std::cout << "Pool stats - Available: " << pool.available()
				<< ", In use: " << pool.inUse() << "\n\n";
		} // end of scope

		// ====================================================================
		// Example 2: Simple Connection Test
		// ====================================================================
		std::cout << "=== Example 2: Simple Connection Test ===\n";
		{
			sqlite_flux::ConnectionPool pool(dbPath, 3, true);

			std::cout << "Acquiring connection...\n";
			{
				auto conn = pool.acquire();
				std::cout << "Connection acquired, running query...\n";

				auto count = conn->getRowCount("users");
				std::cout << "User count: " << (count ? *count : 0) << "\n";

				std::cout << "Connection about to be released...\n";
			} // Connection released here

			std::cout << "Connection released\n";
			std::cout << "Pool available: " << pool.available() << "\n\n";
		} // end of scope

		std::cout << "=== All web examples completed successfully! ===\n";

		return 0;
	}
	catch (const std::exception& e)
	{
		std::cerr << "Fatal error: " << e.what() << "\n";
		return 1;
	} // end of catch
} // end of main