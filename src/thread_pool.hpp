#ifndef _THREAD_POOL_HPP_
#define _THREAD_POOL_HPP_

#include <boost/thread.hpp>
#include <vector>
#include <deque>
#include <memory>
#include <functional>
#include <ctime>


template < typename T >
class SharedQueue : private std::deque<T> {
    typedef typename std::deque<T>   BaseType;
public:
    void push( const T &elem )
    {
        boost::unique_lock<boost::mutex> lk(lock);

        this->push_back( elem );

        lk.unlock();
        condRd.notify_one();
    }

    T pop()
    {
        boost::unique_lock<boost::mutex> lk(lock);
        
        while( this->empty() )
            condRd.wait( lk );

        T retval = this->front();
        this->pop_front();

        return retval;
    }

    void clear()
    {
        boost::unique_lock<boost::mutex> lk(lock);
        BaseType::clear();
    }

protected:
    boost::mutex                  lock;
    boost::condition_variable     condRd;
};


template < typename JobType, typename JobPtr = std::shared_ptr<JobType> >
class ThreadPool {
    typedef SharedQueue<JobPtr>     WorkQueue;
public:
    explicit ThreadPool( std::size_t _Size )
            : m_nSize(_Size)
            , m_arrWorkQueue(_Size)
    {
        ::srand(::time(0));

        for (std::size_t i = 0; i < m_nSize; ++i)
            m_Thrgrp.create_thread( 
                    std::bind(&ThreadPool<JobType, JobPtr>::doWork, this, i) );
    }

    void addJob( const JobPtr &pJob )
    {
        std::size_t idx = ::rand() % m_nSize;
        m_arrWorkQueue[idx].push( pJob );
    }

    void addJob( const JobType &job )
    { addJob( std::make_shared<JobType>(job) ); }

    void terminate()
    {
        for (std::size_t i = 0; i < m_nSize; ++i)
            m_arrWorkQueue[i].push( JobPtr() );
        m_Thrgrp.join_all();
    }

private:
    void doWork( std::size_t i )
    {
        while (true) {
            JobPtr pWork = m_arrWorkQueue[i].pop();
            // 空指针表示结束工作线程
            if (!pWork)
                return;
            (*pWork)();
        } // while
    }

private:
    std::size_t             m_nSize;
    std::vector<WorkQueue>  m_arrWorkQueue;
    boost::thread_group     m_Thrgrp;
};


#endif

