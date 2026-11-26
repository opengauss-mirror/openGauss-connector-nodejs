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

helper('passing options', function (client) {
  it('passes row mode array', function (done) {
    const stream = new QueryStream('SELECT * FROM generate_series(0, 10) num', [], { rowMode: 'array' })
    const query = client.query(stream)
    const result = []
    query.on('data', (datum) => {
      result.push(datum)
    })
    query.on('end', () => {
      const expected = new Array(11).fill(0).map((_, i) => [i])
      assert.deepEqual(result, expected)
      done()
    })
  })

  it('passes custom types', function (done) {
    const types = {
      getTypeParser: () => (string) => string,
    }
    const stream = new QueryStream('SELECT * FROM generate_series(0, 10) num', [], { types })
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
      done()
    })
  })
})
