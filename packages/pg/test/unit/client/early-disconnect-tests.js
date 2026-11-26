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
var helper = require('./test-helper')
var net = require('net')
var pg = require('../../../lib/index.js')

/* console.log() messages show up in `make test` output. TODO: fix it. */
var server = net.createServer(function (c) {
  c.destroy()
  server.close()
})

server.listen(7777, function () {
  var client = new pg.Client('postgres://localhost:7777')
  client.connect(
    assert.calls(function (err) {
      assert(err)
    })
  )
})
