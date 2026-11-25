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

// file for microbenchmarking

import { Writer } from './buffer-writer'
import { serialize } from './index'
import { BufferReader } from './buffer-reader'

const LOOPS = 1000
let count = 0
let start = Date.now()
const writer = new Writer()

const reader = new BufferReader()
const buffer = Buffer.from([33, 33, 33, 33, 33, 33, 33, 0])

const run = () => {
  if (count > LOOPS) {
    console.log(Date.now() - start)
    return
  }
  count++
  for (let i = 0; i < LOOPS; i++) {
    reader.setBuffer(0, buffer)
    reader.cstring()
  }
  setImmediate(run)
}

run()
