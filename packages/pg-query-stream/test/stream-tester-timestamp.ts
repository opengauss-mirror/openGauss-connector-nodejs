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

import helper from './helper'
import QueryStream from '../src'
import spec from 'stream-spec'
import assert from 'assert'

helper('stream tester timestamp', function (client) {
  it('should not warn about max listeners', function (done) {
    const sql = "SELECT * FROM generate_series('1983-12-30 00:00'::timestamp, '2013-12-30 00:00', '1 years')"
    const stream = new QueryStream(sql, [])
    let ended = false
    const query = client.query(stream)
    query.on('end', function () {
      ended = true
    })
    spec(query).readable().pausable({ strict: true }).validateOnExit()
    const checkListeners = function () {
      assert(stream.listeners('end').length < 10)
      if (!ended) {
        setImmediate(checkListeners)
      } else {
        done()
      }
    }
    checkListeners()
  })
})
