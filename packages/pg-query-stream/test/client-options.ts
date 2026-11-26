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

import pg from 'pg'
import assert from 'assert'
import QueryStream from '../src'

describe('client options', function () {
  it('uses custom types from client config', function (done) {
    const types = {
      getTypeParser: () => (string) => string,
    }
    //@ts-expect-error
    const client = new pg.Client({ types })
    client.connect()
    const stream = new QueryStream('SELECT * FROM generate_series(0, 10) num')
    const query = client.query(stream)
    const result = []
    query.on('data', (datum) => {
      result.push(datum)
    })
    query.on('end', () => {
      const expected = new Array(11).fill(0).map((_, i) => ({
        num: i.toString(),
      }))
      assert.deepEqual(result, expected)
      client.end()
      done()
    })
  })
})
