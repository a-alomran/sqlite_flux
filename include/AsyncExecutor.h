// include/AsyncExecutor.h
#pragma once

#include "ConnectionPool.h"
#include "QueryBuilder.h"
#include <coroutine>
#include <future>
#include <functional>
#include <exception>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace sqlite_flux
{

	// ============================================================================
	// Task<T> - Coroutine return type for async operations
	// ============================================================================

	template<typename T>
	class Task
	{
	public:
		struct promise_type
		{
			T value_;
			std::exception_ptr exception_;

			Task get_return_object()
			{
				return Task{ std::coroutine_handle<promise_type>::from_promise(*this) };
			} // end of get_return_object

			std::suspend_never initial_suspend() noexcept { return {}; }
			std::suspend_always final_suspend() noexcept { return {}; }

			void return_value(T value_)
			{
				value_ = std::move(value_);
			} // end of return_value

			void unhandled_exception()
			{
				exception_ = std::current_exception();
			} // end of unhandled_exception
		}; // end of struct promise_type

		using handle_type = std::coroutine_handle<promise_type>;

		explicit Task(handle_type h) : handle_(h) {}

		~Task()
		{
			if (handle_)
			{
				handle_.destroy();
			} // end of if
		} // end of destructor

		// Disable copy, enable move
		Task(const Task&) = delete;
		Task& operator=(const Task&) = delete;

		Task(Task&& other) noexcept : handle_(other.handle_)
		{
			other.handle_ = nullptr;
		} // end of move constructor

		Task& operator=(Task&& other) noexcept
		{
			if (this != &other)
			{
				if (handle_)
				{
					handle_.destroy();
				} // end of if
				handle_ = other.handle_;
				other.handle_ = nullptr;
			} // end of if
			return *this;
		} // end of move assignment

		// Awaiter interface for co_await support
		bool await_ready() const noexcept
		{
			return handle_ && handle_.done();
		} // end of await_ready

		void await_suspend(std::coroutine_handle<> awaiting) noexcept
		{
			// Task executes immediately, so just resume the awaiting coroutine
		} // end of await_suspend

		T await_resume()
		{
			if (!handle_)
			{
				throw std::runtime_error("Invalid task handle");
			} // end of if

			if (handle_.promise().exception_)
			{
				std::rethrow_exception(handle_.promise().exception_);
			} // end of if

			return std::move(handle_.promise().value_);
		} // end of await_resume

		// Get the result (blocks until ready)
		T get()
		{
			return await_resume();
		} // end of get

		// Check if task is ready
		bool ready() const noexcept
		{
			return await_ready();
		} // end of ready

	private:
		handle_type handle_;
	}; // end of class Task

	// ============================================================================
	// Task<void> specialization
	// ============================================================================

	template<>
	class Task<void>
	{
	public:
		struct promise_type
		{
			std::exception_ptr exception_;

			Task get_return_object()
			{
				return Task{ std::coroutine_handle<promise_type>::from_promise(*this) };
			} // end of get_return_object

			std::suspend_never initial_suspend() noexcept { return {}; }
			std::suspend_always final_suspend() noexcept { return {}; }

			void return_void() noexcept {}

			void unhandled_exception()
			{
				exception_ = std::current_exception();
			} // end of unhandled_exception
		}; // end of struct promise_type

		using handle_type = std::coroutine_handle<promise_type>;

		explicit Task(handle_type h) : handle_(h) {}

		~Task()
		{
			if (handle_)
			{
				handle_.destroy();
			} // end of if
		} // end of destructor

		// Disable copy, enable move
		Task(const Task&) = delete;
		Task& operator=(const Task&) = delete;

		Task(Task&& other) noexcept : handle_(other.handle_)
		{
			other.handle_ = nullptr;
		} // end of move constructor

		Task& operator=(Task&& other) noexcept
		{
			if (this != &other)
			{
				if (handle_)
				{
					handle_.destroy();
				} // end of if
				handle_ = other.handle_;
				other.handle_ = nullptr;
			} // end of if
			return *this;
		} // end of move assignment

		// Awaiter interface for co_await support
		bool await_ready() const noexcept
		{
			return handle_ && handle_.done();
		} // end of await_ready

		void await_suspend(std::coroutine_handle<> awaiting) noexcept
		{
			// Task executes immediately, so just resume the awaiting coroutine
		} // end of await_suspend

		void await_resume()
		{
			if (!handle_)
			{
				throw std::runtime_error("Invalid task handle");
			} // end of if

			if (handle_.promise().exception_)
			{
				std::rethrow_exception(handle_.promise().exception_);
			} // end of if
		} // end of await_resume

		void get()
		{
			await_resume();
		} // end of get

		bool ready() const noexcept
		{
			return await_ready();
		} // end of ready

	private:
		handle_type handle_;
	}; // end of class Task<void>

	// ============================================================================
	// ThreadPool - Worker thread pool for async operations
	// ============================================================================

	class ThreadPool
	{
	public:
		explicit ThreadPool(size_t numThreads = std::thread::hardware_concurrency());
		~ThreadPool();

		// Disable copy and move
		ThreadPool(const ThreadPool&) = delete;
		ThreadPool& operator=(const ThreadPool&) = delete;
		ThreadPool(ThreadPool&&) = delete;
		ThreadPool& operator=(ThreadPool&&) = delete;

		// Enqueue a task for execution
		template<typename F, typename... Args>
		auto enqueue(F&& f, Args&&... args) -> std::future<typename std::invoke_result<F, Args...>::type>;

		// Get number of worker threads
		size_t size() const { return workers_.size(); }

		// Get number of pending tasks
		size_t pendingTasks() const;

	private:
		void workerThread();

		std::vector<std::thread> workers_;
		std::queue<std::function<void()>> tasks_;

		mutable std::mutex queueMutex_;
		std::condition_variable condition_;
		std::atomic<bool> stop_{ false };
	}; // end of class ThreadPool

	// ============================================================================
	// AsyncExecutor - High-level async API for database operations
	// ============================================================================

	class AsyncExecutor
	{
	public:
		explicit AsyncExecutor(ConnectionPool& pool, size_t threadPoolSize = 4);
		~AsyncExecutor();

		// Disable copy and move
		AsyncExecutor(const AsyncExecutor&) = delete;
		AsyncExecutor& operator=(const AsyncExecutor&) = delete;
		AsyncExecutor(AsyncExecutor&&) = delete;
		AsyncExecutor& operator=(AsyncExecutor&&) = delete;

		// Async query operations
		Task<ResultSet> query(const std::string& sql);
		Task<ResultSet> selectAll(const std::string& tableName);
		Task<ResultSet> selectWhere(const std::string& tableName, const std::string& whereClause);

		// Async execute operations
		Task<bool> execute(const std::string& sql);

		// Async transaction operations
		Task<bool> beginTransaction();
		Task<bool> commit();
		Task<bool> rollback();

		// Async count operation
		Task<int64_t> count(const std::string& tableName);

		// Async exists check
		Task<bool> exists(const std::string& tableName, const std::string& whereClause);

		// Get pool statistics
		size_t availableConnections() const;
		size_t pendingOperations() const;

	private:
		ConnectionPool& pool_;
		ThreadPool threadPool_;
	}; // end of class AsyncExecutor

	// ============================================================================
	// Template implementation for ThreadPool::enqueue
	// ============================================================================

	template<typename F, typename... Args>
	auto ThreadPool::enqueue(F&& f, Args&&... args) -> std::future<typename std::invoke_result<F, Args...>::type>
	{
		using return_type = typename std::invoke_result<F, Args...>::type;

		auto task = std::make_shared<std::packaged_task<return_type()>>(
			std::bind(std::forward<F>(f), std::forward<Args>(args)...)
		);

		std::future<return_type> result = task->get_future();

		{
			std::lock_guard lock(queueMutex_);

			if (stop_.load(std::memory_order_acquire))
			{
				throw std::runtime_error("ThreadPool is stopped");
			} // end of if

			tasks_.emplace([task]() { (*task)(); });
		} // end of lock scope

		condition_.notify_one();
		return result;
	} // end of enqueue

} // namespace sqlite_flux