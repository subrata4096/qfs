//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2006/03/16
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

#ifndef _CHUNKSERVER_H
#define _CHUNKSERVER_H

#include "NetThreadClient.h"
#include "ChunkManager.h"
#include "ClientManager.h"
#include "ClientSM.h"
#include "MetaServerSM.h"
#include "RemoteSyncSM.h"
#include "qcdio/QCMutex.h"
#include "kfsio/NetConnection.h"

namespace KFS
{
using std::string;

// Chunk server globals and main event loop.
class ChunkServer
{
private:
public:
    ChunkServer()
        : mOpCount(0),
          mUpdateServerIpFlag(false),
          mLocation(),
          mRemoteSyncers(),
          mMutex(0),
          mNetThreads(0)
        {}
    ~ChunkServer();
    bool Init(int clientAcceptPort, const string& serverIp);
    bool MainLoop(int threadCount, int firstCpuIndex);
    bool IsLocalServer(const ServerLocation& location) const {
        return mLocation == location;
    }
    RemoteSyncSMPtr FindServer(const ServerLocation& location,
                               bool connect = true);
    bool RemoveServer(RemoteSyncSM& target);
    string GetMyLocation() const {
        return mLocation.ToString();
    }
    const ServerLocation& GetLocation() const {
        return mLocation;
    }
    void OpInserted() {
        mOpCount++;
    }
    void OpFinished() {
        mOpCount--;
        if (mOpCount < 0) {
            mOpCount = 0;
        }
    }
    int GetNumOps() const {
        return mOpCount;
    }
    void SendTelemetryReport(KfsOp_t op, double timeSpent);
    bool CanUpdateServerIp() const {
        return mUpdateServerIpFlag;
    }
    inline void SetLocation(const ServerLocation& loc);
    QCMutex* GetMutex()
        { return mMutex; }
    bool Enqueue(RemoteSyncSM& sm, KfsOp* op)
        { return (mNetThreads && EnqueueSelf(sm, op)); }
    KfsCallbackObj* NewNetConnection(
        NetThreadClient&  netThreadClient,
        KfsCallbackObj*   client,
        NetConnectionPtr& connection)
    {
        return (mNetThreads ?
            NewNetConnectionSelf(netThreadClient, client, connection) :
            client
        );
    }
    void AddConnection(
        NetThreadClient&        netThreadClient,
        const NetConnectionPtr& connection);
    bool CmdDone(ClientSM& sm, KfsOp* op)
        { return (mNetThreads && CmdDoneSelf(sm, op)); }
    bool ScheduleNetRead(
        NetThreadClient&        netThreadClient,
        const NetConnectionPtr& connection)
    {
        return (mNetThreads &&
            ScheduleNetReadSelf(netThreadClient, connection));
    }
    void StartFlush(
        NetThreadClient&        netThreadClient,
        const NetConnectionPtr& connection)
    {
        if (mNetThreads) {
            StartFlushSelf(netThreadClient, connection);
        } else {
            connection->StartFlush();
        }
    }
private:
    typedef NetThreadClient::NetThread     NetThread;
    typedef RemoteSyncSM::RemoteSyncSMList RemoteSyncSMList;
    // # of ops in the system
    int              mOpCount;
    bool             mUpdateServerIpFlag;
    ServerLocation   mLocation;
    RemoteSyncSMList mRemoteSyncers;
    QCMutex*         mMutex;
    NetThread*       mNetThreads;
    struct {
        size_t mStorage[
            (sizeof(QCMutex) + sizeof(size_t) - 1) / sizeof(size_t)];
    }                mMutexStorage;
    KfsCallbackObj* NewNetConnectionSelf(
        NetThreadClient&  netThreadClient,
        KfsCallbackObj*   client,
        NetConnectionPtr& connection);
    bool CmdDoneSelf(ClientSM& sm, KfsOp* op);
    bool EnqueueSelf(RemoteSyncSM& sm, KfsOp* op);
    bool ScheduleNetReadSelf(
        NetThreadClient&        netThreadClient,
        const NetConnectionPtr& connection);
    void StartFlushSelf(
        NetThreadClient&        netThreadClient,
        const NetConnectionPtr& connection);
private:
    // No copy.
    ChunkServer(const ChunkServer&);
    ChunkServer& operator=(const ChunkServer&);
};

extern ChunkServer gChunkServer;
}

#endif // _CHUNKSERVER_H
