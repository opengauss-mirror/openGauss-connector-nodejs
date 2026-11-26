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

helper('empty-query', function (client) {
  it('handles empty query', function (done) {
    const stream = new QueryStream('-- this is a comment', [])
    const query = client.query(stream)
    query
      .on('end', function () {
        // nothing should happen for empty query
        done()
      })
      .on('data', function () {
        // noop to kick off reading
      })
  })

  it('continues to function after stream', function (done) {
    client.query('SELECT NOW()', done)
  })
})
