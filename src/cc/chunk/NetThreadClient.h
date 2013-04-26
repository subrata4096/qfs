//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id$
//
// Created 2013/04/23
// Author: Mike Ovsiannikov
//
// Copyright 2013 Quantcast Corp.
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

#ifndef CHUNKSERVER_NETTHREADCLIENT_H
#define CHUNKSERVER_NETTHREADCLIENT_H

namespace KFS
{

class NetThreadClient
{
public:
class NetThread;
protected:
    NetThreadClient()
        : mNetThread(0)
        {}
private:
    NetThread* mNetThread;
    friend class NetThread;
};

}

#endif /* CHUNKSERVER_NETTHREADCLIENT_H */
