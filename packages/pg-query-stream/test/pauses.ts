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
import concat from 'concat-stream'
import tester from 'stream-tester'
import JSONStream from 'JSONStream'
import QueryStream from '../src'

helper('pauses', function (client) {
  it('pauses', function (done) {
    this.timeout(5000)
    const stream = new QueryStream('SELECT * FROM generate_series(0, $1) num', [200], {
      batchSize: 2,
      highWaterMark: 2,
    })
    const query = client.query(stream)
    const pauser = tester.createPauseStream(0.1, 100)
    query
      .pipe(JSONStream.stringify())
      .pipe(pauser)
      .pipe(
        concat(function (json) {
          JSON.parse(json)
          done()
        })
      )
  })
})
