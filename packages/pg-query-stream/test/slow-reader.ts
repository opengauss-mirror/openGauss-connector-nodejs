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
