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
import QueryStream from '../src'

describe('stream config options', () => {
  // this is mostly for backwards compatibility.
  it('sets readable.highWaterMark based on batch size', () => {
    const stream = new QueryStream('SELECT NOW()', [], {
      batchSize: 88,
    })
    assert.equal(stream.readableHighWaterMark, 88)
  })

  it('sets readable.highWaterMark based on highWaterMark config', () => {
    const stream = new QueryStream('SELECT NOW()', [], {
      highWaterMark: 88,
    })

    assert.equal(stream.readableHighWaterMark, 88)
  })

  it('defaults to 100 for highWaterMark', () => {
    const stream = new QueryStream('SELECT NOW()', [])

    assert.equal(stream.readableHighWaterMark, 100)
  })
})
