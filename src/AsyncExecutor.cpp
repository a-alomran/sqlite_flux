// src/AsyncExecutor.cpp
#include "AsyncExecutor.h"
#include <stdexcept>

namespace sqlite_flux
{

	// ============================================================================
	// ThreadPool implementation
	// ============================================================================

	ThreadPool::ThreadPool(size_t numThreads)
	{
		if (numThreads == 0)
		{
			throw std::invalid_argument("Thread pool size must be greater than 0");
		} // end of if

		for (size_t i = 0; i < numThreads; ++i)
		{
			workers_.emplace_back([this] { workerThread(); });
		} // end of for
	} // end of ThreadPool constructor

	ThreadPool::~ThreadPool()
	{
		stop_.store(true, std::memory_order_release);
		condition_.notify_all();

		for (std::thread& worker : workers_)
		{
			if (worker.joinable())
			{
				worker.join();
			} // end of if
		} // end of for
	} // end of ThreadPool destructor

	void ThreadPool::workerThread()
	{
		while (true)
		{
			std::function<void()> task;

			{
				std::unique_lock lock(queueMutex_);

				condition_.wait(lock, [this] {
					return stop_.load(std::memory_order_acquire) || !tasks_.empty();
					});

				if (stop_.load(std::memory_order_acquire) && tasks_.empty())
				{
					return;  // Exit thread
				} // end of if

				if (!tasks_.empty())
				{
					task = std::move(tasks_.front());
					tasks_.pop();
				} // end of if
			} // end of lock scope

			if (task)
			{
				task();
			} // end of if
		} // end of while
	} // end of workerThread

	size_t ThreadPool::pendingTasks() const
	{
		std::lock_guard lock(queueMutex_);
		return tasks_.size();
	} // end of pendingTasks

	// ============================================================================
	// AsyncExecutor implementation
	// ============================================================================

	AsyncExecutor::AsyncExecutor(ConnectionPool& pool, size_t threadPoolSize)
		: pool_(pool), threadPool_(threadPoolSize)
	{
	} // end of AsyncExecutor constructor

	AsyncExecutor::~AsyncExecutor() = default;

	Task<ResultSet> AsyncExecutor::query(const std::string& sql)
	{
		auto future = threadPool_.enqueue([this, sql]() {
			auto conn = pool_.acquire();
			return conn->query(sql);
			});

		co_return future.get();
	} // end of query

	Task<ResultSet> AsyncExecutor::selectAll(const std::string& tableName)
	{
		auto future = threadPool_.enqueue([this, tableName]() {
			auto conn = pool_.acquire();
			return conn->selectAll(tableName);
			});

		co_return future.get();
	} // end of selectAll

	Task<ResultSet> AsyncExecutor::selectWhere(const std::string& tableName, const std::string& whereClause)
	{
		auto future = threadPool_.enqueue([this, tableName, whereClause]() {
			auto conn = pool_.acquire();
			return conn->selectWhere(tableName, whereClause);
			});

		co_return future.get();
	} // end of selectWhere

	Task<bool> AsyncExecutor::execute(const std::string& sql)
	{
		auto future = threadPool_.enqueue([this, sql]() {
			auto conn = pool_.acquire();
			return conn->execute(sql);
			});

		co_return future.get();
	} // end of execute

	Task<bool> AsyncExecutor::beginTransaction()
	{
		auto future = threadPool_.enqueue([this]() {
			auto conn = pool_.acquire();
			return conn->beginTransaction();
			});

		co_return future.get();
	} // end of beginTransaction

	Task<bool> AsyncExecutor::commit()
	{
		auto future = threadPool_.enqueue([this]() {
			auto conn = pool_.acquire();
			return conn->commit();
			});

		co_return future.get();
	} // end of commit

	Task<bool> AsyncExecutor::rollback()
	{
		auto future = threadPool_.enqueue([this]() {
			auto conn = pool_.acquire();
			return conn->rollback();
			});

		co_return future.get();
	} // end of rollback

	Task<int64_t> AsyncExecutor::count(const std::string& tableName)
	{
		auto future = threadPool_.enqueue([this, tableName]() -> int64_t {
			auto conn = pool_.acquire();
			auto result = conn->getRowCount(tableName);
			return result.value_or(0);
			});

		co_return future.get();
	} // end of count

	Task<bool> AsyncExecutor::exists(const std::string& tableName, const std::string& whereClause)
	{
		auto future = threadPool_.enqueue([this, tableName, whereClause]() -> bool {
			auto conn = pool_.acquire();
			QueryFactory factory(*conn);
			return factory.FromTable(tableName)
				.Filter(whereClause, int64_t(1))
				.Any();
			});

		co_return future.get();
	} // end of exists

	size_t AsyncExecutor::availableConnections() const
	{
		return pool_.available();
	} // end of availableConnections

	size_t AsyncExecutor::pendingOperations() const
	{
		return threadPool_.pendingTasks();
	} // end of pendingOperations

} // namespace sqlite_flux