/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/mit
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict'
const net = require('net')
const pg = require('../../../lib/index.js')
const helper = require('./test-helper')

const suite = new helper.Suite()

suite.test('setting keep alive', (done) => {
  const server = net.createServer((c) => {
    c.destroy()
    server.close()
  })

  server.listen(7777, () => {
    const stream = new net.Socket()
    stream.setKeepAlive = (enable, initialDelay) => {
      assert(enable === true)
      assert(initialDelay === 10000)
      done()
    }

    const client = new pg.Client({
      host: 'localhost',
      port: 7777,
      keepAlive: true,
      keepAliveInitialDelayMillis: 10000,
      stream,
    })

    client.connect().catch(() => {})
  })
})
