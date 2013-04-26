//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/23
// Author: Sriram Rao
//
// Copyright 2008-2012 Quantcast Corp.
// Copyright 2006-2008 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
//
// Licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
//
//----------------------------------------------------------------------------

#include "kfsio/Globals.h"

#include "ChunkServer.h"
#include "Logger.h"
#include "utils.h"
#include "qcdio/qcstutils.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace KFS {

using std::string;
using std::make_pair;
using libkfsio::globalNetManager;


ChunkServer gChunkServer;

class NetThreadClient::NetThread :
    private QCRunnable,
    private ITimeout
{
public:
    NetThread(
        NetThread* next,
        int        cpuIndex,
        QCMutex&   mutex)
        : QCRunnable(),
          mNext(next),
          mThread(),
          mMutex(mutex),
          mNetManager(),
          mAddQueue(),
          mNetReadQueue(),
          mEnqueueQueue(),
          mDoneQueue(),
          mRemoveQueue(),
          mCurAddQueue(),
          mCurNetReadQueue(),
          mCurEnqueueQueue(),
          mCurDoneQueue(),
          mCurRemoveQueue(),
          mPendingFlush()
    {
        mAddQueue.reserve(4 << 10);
        mNetReadQueue.reserve(4 << 10);
        mEnqueueQueue.reserve(4 << 10);
        mDoneQueue.reserve(4 << 10);
        mRemoveQueue.reserve(4 << 10);
        mCurAddQueue.reserve(4 << 10);
        mCurNetReadQueue.reserve(4 << 10);
        mCurEnqueueQueue.reserve(4 << 10);
        mCurDoneQueue.reserve(4 << 10);
        mCurRemoveQueue.reserve(4 << 10);
        mNetManager.RegisterTimeoutHandler(this);
        const size_t kStackSize = 256 << 10;
        mThread.Start(this, kStackSize, "NetIO",
            QCThread::CpuAffinity(cpuIndex));
    }
    virtual ~NetThread()
    {
        assert(! mThread.IsStarted());
    }
    void Stop()
    {
        {
            QCStMutexLocker lock(mMutex);
            mNetManager.Shutdown();
        }
        mNetManager.Wakeup();
        mThread.Join();
        mNetManager.UnRegisterTimeoutHandler(this);
        QCStMutexLocker lock(mMutex);
    }
    void Cleanup()
    {
        assert(! mThread.IsStarted());
        do {
            while (! mAddQueue.empty()) {
                NetConnectionPtr& conn = mCurAddQueue.back();
                if (conn->IsGood()) {
                    conn->HandleErrorEvent();
                }
                mAddQueue.pop_back();
            }
        } while (RunQueues());
    }
    virtual void Timeout()
    {
        if (mNetManager.IsRunning()) {
            RunQueues();
        }
    }
    virtual void Run()
        { mNetManager.MainLoop(); }
    void AddConnection(
        NetThreadClient&        netThreadClient,
        const NetConnectionPtr& connection)
    {
        assert(mMutex.IsOwned());
        if (! mNetManager.IsRunning()) {
            if (connection->IsGood()) {
                connection->HandleErrorEvent();
            }
            return;
        }
        const bool wakeupFlag = mAddQueue.empty();
        netThreadClient.mNetThread = this;
        mAddQueue.push_back(connection);
        if (wakeupFlag) {
            mNetManager.Wakeup();
        }
    }
    NetThread* GetNext() const
        { return mNext; }
    void SetNext(NetThread* next)
        { mNext = next; }
    static bool CmdDone(ClientSM& sm, KfsOp* op)
    {
        if (! sm.mNetThread || sCurThread == sm.mNetThread) {
            return false;
        }
        sm.mNetThread->CmdDoneSelf(sm, op);
        return true;
    }
    static bool Enqueue(RemoteSyncSM& sm, KfsOp* op)
    {
        if (! sm.mNetThread || sCurThread == sm.mNetThread) {
            return false;
        }
        sm.mNetThread->EnqueueSelf(sm, op);
        return true;
    }
    static bool Remove(RemoteSyncSM& sm)
    {
        if (! sm.mNetThread || sCurThread == sm.mNetThread) {
            sm.mNetThread = 0;
            return false;
        }
        sm.mNetThread->RemoveSelf(sm);
        return true;
    }
    static bool ScheduleNetRead(
        NetThreadClient&        netThreadClient,
        const NetConnectionPtr& connection)
    {
        if (! netThreadClient.mNetThread ||
                sCurThread == netThreadClient.mNetThread) {
            return false;
        }
        netThreadClient.mNetThread->ScheduleNetReadSelf(connection);
        return true;
    }
    static void StartFlush(
        NetThreadClient&        netThreadClient,
        const NetConnectionPtr& connection)
    {
        if (! netThreadClient.mNetThread) {
            connection->StartFlush();
        }
        if (connection->CanStartFlush()) {
            netThreadClient.mNetThread->StartFlushSelf(connection);
        }
    }
private:
    typedef vector<NetConnectionPtr>               AddQueue;
    typedef AddQueue                               NetReadQueue;
    typedef vector<pair<RemoteSyncSMPtr, KfsOp*> > OpEnqueueQueue;
    typedef vector<pair<ClientSM*,       KfsOp*> > OpDoneQueue;
    typedef vector<RemoteSyncSMPtr>                RemoveQueue;
    typedef set<
        NetConnectionPtr,
        less<NetConnectionPtr>,
        StdFastAllocator<NetConnectionPtr>
    > PendingFlush;

    NetThread*     mNext;
    QCThread       mThread;
    QCMutex&       mMutex;
    NetManager     mNetManager;
    AddQueue       mAddQueue;
    NetReadQueue   mNetReadQueue;
    OpEnqueueQueue mEnqueueQueue;
    OpDoneQueue    mDoneQueue;
    RemoveQueue    mRemoveQueue;
    AddQueue       mCurAddQueue;
    NetReadQueue   mCurNetReadQueue;
    OpEnqueueQueue mCurEnqueueQueue;
    OpDoneQueue    mCurDoneQueue;
    RemoveQueue    mCurRemoveQueue;
    PendingFlush   mPendingFlush;

    static NetThread* sCurThread;

    void EnqueueSelf(RemoteSyncSM& sm, KfsOp* op)
    {
        const bool wakeupFlag = mEnqueueQueue.empty();
        mEnqueueQueue.push_back(make_pair(sm.shared_from_this(), op));
        if (wakeupFlag) {
            mNetManager.Wakeup();
        }
    }
    void CmdDoneSelf(ClientSM& sm, KfsOp* op)
    {
        const bool wakeupFlag = mDoneQueue.empty();
        mDoneQueue.push_back(make_pair(&sm, op));
        if (wakeupFlag) {
            mNetManager.Wakeup();
        }
    }
    void RemoveSelf(RemoteSyncSM& sm)
    {
        const bool wakeupFlag = mRemoveQueue.empty();
        mRemoveQueue.push_back(sm.shared_from_this());
        if (wakeupFlag) {
            mNetManager.Wakeup();
        }
    }
    void ScheduleNetReadSelf(const NetConnectionPtr& connection)
    {
        const bool wakeupFlag = mNetReadQueue.empty();
        mNetReadQueue.push_back(connection);
        if (wakeupFlag) {
            mNetManager.Wakeup();
        }
    }
    void StartFlushSelf(const NetConnectionPtr& connection)
    {
        // Wakeup isn't strictly required in most cases, as the timers run
        // before the net manager invokes poll. It is here to handle the
        // corner case where flush is invoked from another timer.
        const bool wakeupFlag = mPendingFlush.empty();
        mPendingFlush.insert(connection);
        if (wakeupFlag) {
            mNetManager.Wakeup();
        }
    }
    bool RunQueues()
    {
        bool processedFlag;
        {
            QCStMutexLocker lock(mMutex);
            assert(! sCurThread);
            QCStValueChanger<NetThread*> changer(sCurThread, this);

            processedFlag = ! mDoneQueue.empty();
            assert(mCurDoneQueue.empty());
            mCurDoneQueue.swap(mDoneQueue);
            for (OpDoneQueue::const_iterator it = mCurDoneQueue.begin();
                    it != mCurDoneQueue.end();
                    ++it) {
                it->first->HandleEvent(EVENT_CMD_DONE, it->second);
            }
            mCurDoneQueue.clear();

            processedFlag = processedFlag || ! mEnqueueQueue.empty();
            assert(mCurEnqueueQueue.empty());
            mCurEnqueueQueue.swap(mEnqueueQueue);
            for (OpEnqueueQueue::const_iterator it = mCurEnqueueQueue.begin();
                    it != mCurEnqueueQueue.end();
                    ++it) {
                it->first->Enqueue(it->second);
            }
            mCurEnqueueQueue.clear();

            processedFlag = processedFlag || ! mRemoveQueue.empty();
            assert(mCurRemoveQueue.empty());
            mCurRemoveQueue.swap(mRemoveQueue);
            for (RemoveQueue::const_iterator it = mCurRemoveQueue.begin();
                    it != mCurRemoveQueue.end();
                    ++it) {
                (*it)->Finish();
            }
            mCurRemoveQueue.clear();

            // Do not release the mutex, as event handler will have to
            // re-acquire it.
            processedFlag = processedFlag || ! mNetReadQueue.empty();
            assert(mCurNetReadQueue.empty());
            mCurNetReadQueue.swap(mNetReadQueue);
            for (NetReadQueue::const_iterator it = mCurNetReadQueue.begin();
                    it != mCurNetReadQueue.end();
                    ++it) {
                const NetConnectionPtr& cur = *it;
                if (cur->IsGood() && cur->GetCallback()) {
                    cur->GetCallback()->HandleEvent(
                        EVENT_NET_READ, &(cur->GetInBuffer()));
                }
            }
            mCurNetReadQueue.clear();

            processedFlag = processedFlag || ! mAddQueue.empty();
            assert(mCurAddQueue.empty());
            mCurAddQueue.swap(mAddQueue);
        }
        for (AddQueue::const_iterator it = mCurAddQueue.begin();
                it != mCurAddQueue.end();
                ++it) {
            mNetManager.AddConnection(*it);
        }
        mCurAddQueue.clear();

        // Run pending flush queue with no mutex held to do network io in
        // parallel with other threads.
        processedFlag = processedFlag || mPendingFlush.empty();
        PendingFlush curPendingFlush;
        curPendingFlush.swap(mPendingFlush);
        for (PendingFlush::const_iterator it = curPendingFlush.begin();
                it != curPendingFlush.end();
                ++it) {
            (*it)->StartFlush();
        }
        return processedFlag;
    }
};
NetThreadClient::NetThread* NetThreadClient::NetThread::sCurThread(0);

ChunkServer::~ChunkServer()
{
    assert(! mMutex && ! mNetThreads);
}

void
ChunkServer::SendTelemetryReport(KfsOp_t /* op */, double /* timeSpent */)
{
}

bool
ChunkServer::Init(int clientAcceptPort, const string& serverIp)
{
    if (clientAcceptPort < 0) {
        KFS_LOG_STREAM_FATAL <<
            "invalid client port: " << clientAcceptPort <<
        KFS_LOG_EOM;
        return false;
    }
    mUpdateServerIpFlag = serverIp.empty();
    if (! mUpdateServerIpFlag) {
        // For now support only ipv4 addresses.
        // The ip does not have to be assigned to any local NICs.
        // The ip is valid as long as the clients can reach this particular
        // process using this ip.
        //
        // In the case when the chunk server is on the same host as the meta
        // server, but the clients aren't, the server ip must be specified.
        // Setting cnchunkServer.metaServer.hostname to the client "visible" ip
        // might also work.
        //
        // This also allows to work with NAT between the clients, and chunk and
        // meta servers.
        // The server ip can also be used for the testing purposes, so that the
        // clients always fail to connect to the chunk server, but the meta
        // server considers this server operational.
        struct in_addr addr;
        if (! inet_aton(serverIp.c_str(), &addr)) {
            KFS_LOG_STREAM_FATAL <<
                "invalid server ip: " << serverIp <<
            KFS_LOG_EOM;
            return false;
        }
    }
    if (! gClientManager.BindAcceptor(clientAcceptPort) ||
            gClientManager.GetPort() <= 0) {
        KFS_LOG_STREAM_FATAL <<
            "failed to bind acceptor to port: " << clientAcceptPort <<
        KFS_LOG_EOM;
        return false;
    }
    mLocation.Reset(serverIp.c_str(), gClientManager.GetPort());
    return true;
}

bool
ChunkServer::MainLoop(int threadCount, int firstCpuIndex)
{
    if (gChunkManager.Restart() != 0) {
        return false;
    }
    gLogger.Start();
    gChunkManager.Start();
    if (! gClientManager.StartListening()) {
        KFS_LOG_STREAM_FATAL <<
            "failed to start acceptor on port: " << gClientManager.GetPort() <<
        KFS_LOG_EOM;
        return false;
    }
    if (0 < threadCount) {
        mMutex = new (&mMutexStorage) QCMutex();
    }
    NetThread* first = 0;
    for (int i = 0; i < threadCount; i++) {
        mNetThreads = new NetThread(
            mNetThreads,
            0 < firstCpuIndex ? firstCpuIndex + i : firstCpuIndex,
            *mMutex
        );
        if (! first) {
            first = mNetThreads;
        }
    }
    if (mNetThreads) {
        // Circular list.
        mNetThreads->SetNext(first);
    }
    gMetaServerSM.Init();

    globalNetManager().MainLoop(mMutex);
    if (mNetThreads) {
        NetThread* const head = mNetThreads;
        NetThread*       next = head;
        do {
            NetThread* const cur = next;
            next = cur->GetNext(); 
            cur->Stop();
        } while (next != head);
        do {
            NetThread* const cur = next;
            next = cur->GetNext(); 
            cur->Cleanup();
        } while (next != head);
        mNetThreads = 0;
        do {
            NetThread* const cur = next;
            next = cur->GetNext(); 
            delete cur;
        } while (next != head);
    }
    if (mMutex) {
        mMutex->~QCMutex();
        mMutex = 0;
    }

    RemoteSyncSMList serversToRelease;
    mRemoteSyncers.swap(serversToRelease);
    RemoteSyncSM::ReleaseAllServers(serversToRelease);

    return true;
}

void
ChunkServer::AddConnection(
    NetThreadClient&        netThreadClient,
    const NetConnectionPtr& connection)
{
    if (! mNetThreads) {
        globalNetManager().AddConnection(connection);
        return;
    }
    mNetThreads->AddConnection(netThreadClient, connection);
    mNetThreads = mNetThreads->GetNext();
}

KfsCallbackObj*
ChunkServer::NewNetConnectionSelf(
    NetThreadClient&  netThreadClient,
    KfsCallbackObj*   client,
    NetConnectionPtr& connection)
{
    assert(mMutex && mNetThreads);
    NetConnectionPtr const conn = connection;
    connection.reset(); // Take ownership.
    conn->SetOwningKfsCallbackObj(client);
    AddConnection(netThreadClient, conn);
    return 0;
}

bool
ChunkServer::CmdDoneSelf(ClientSM& sm, KfsOp* op)
{
    return NetThread::CmdDone(sm, op);
}

bool
ChunkServer::EnqueueSelf(RemoteSyncSM& sm, KfsOp* op)
{
    return NetThread::Enqueue(sm, op);
}

bool
ChunkServer::ScheduleNetReadSelf(
    NetThreadClient&        netThreadClient,
    const NetConnectionPtr& connection)
{
    return NetThread::ScheduleNetRead(netThreadClient, connection);
}

void
ChunkServer::StartFlushSelf(
    NetThreadClient&        netThreadClient,
    const NetConnectionPtr& connection)
{
    return NetThread::StartFlush(netThreadClient, connection);
}

void
StopNetProcessor(int /* status */)
{
    globalNetManager().Shutdown();
}

RemoteSyncSMPtr
ChunkServer::FindServer(const ServerLocation &location, bool connect)
{
    return RemoteSyncSM::FindServer(mRemoteSyncers, location, connect);
}

bool
ChunkServer::RemoveServer(RemoteSyncSM& target)
{
    const bool ret = NetThread::Remove(target);
    RemoteSyncSM::RemoveServer(mRemoteSyncers, &target);
    return ret;
}

}
