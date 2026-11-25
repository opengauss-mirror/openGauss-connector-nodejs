/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
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
