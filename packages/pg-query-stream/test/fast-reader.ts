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

import assert from 'assert'
import helper from './helper'
import QueryStream from '../src'

helper('fast reader', function (client) {
  it('works', function (done) {
    const stream = new QueryStream('SELECT * FROM generate_series(0, 200) num', [])
    const query = client.query(stream)
    const result = []
    stream.on('readable', function () {
      let res = stream.read()
      while (res) {
        if (result.length !== 201) {
          assert(res, 'should not return null on evented reader')
        } else {
          // a readable stream will emit a null datum when it finishes being readable
          // https://nodejs.org/api/stream.html#stream_event_readable
          assert.equal(res, null)
        }
        if (res) {
          result.push(res.num)
        }
        res = stream.read()
      }
    })
    stream.on('end', function () {
      const total = result.reduce(function (prev, cur) {
        return prev + cur
      })
      assert.equal(total, 20100)
      done()
    })
    assert.strictEqual(query.read(2), null)
  })
})
