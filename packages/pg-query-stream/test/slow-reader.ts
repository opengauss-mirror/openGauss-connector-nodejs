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

import helper from './helper'
import QueryStream from '../src'
import concat from 'concat-stream'

import { Transform } from 'stream'

const mapper = new Transform({ objectMode: true })

mapper._transform = function (obj, enc, cb) {
  this.push(obj)
  setTimeout(cb, 5)
}

helper('slow reader', function (client) {
  it('works', function (done) {
    this.timeout(50000)
    const stream = new QueryStream('SELECT * FROM generate_series(0, 201) num', [], {
      highWaterMark: 100,
      batchSize: 50,
    })
    stream.on('end', function () {
      // console.log('stream end')
    })
    client.query(stream)
    stream.pipe(mapper).pipe(
      concat(function (res) {
        done()
      })
    )
  })
})
