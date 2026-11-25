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

'use strict'

const async = require('async')

class Test {
  constructor(name, cb) {
    this.name = name
    this.action = cb
    this.timeout = 5000
  }

  run(cb) {
    try {
      this._run(cb)
    } catch (e) {
      cb(e)
    }
  }

  _run(cb) {
    if (!this.action) {
      console.log(`${this.name} skipped`)
      return cb()
    }
    if (!this.action.length) {
      const result = this.action.call(this)
      if (!(result || 0).then) {
        return cb()
      }
      result.then(() => cb()).catch((err) => cb(err || new Error('Unhandled promise rejection')))
    } else {
      this.action.call(this, cb)
    }
  }
}

class Suite {
  constructor(name) {
    console.log('')
    this._queue = async.queue(this.run.bind(this), 1)
    this._queue.drain = () => {}
  }

  run(test, cb) {
    process.stdout.write('  ' + test.name + ' ')
    if (!test.action) {
      process.stdout.write('? - SKIPPED\n')
      return cb()
    }

    const tid = setTimeout(() => {
      const err = Error(`test: ${test.name} did not complete withint ${test.timeout}ms`)
      console.log('\n' + err.stack)
      process.exit(-1)
    }, test.timeout)

    test.run((err) => {
      clearTimeout(tid)
      if (err) {
        process.stdout.write(`FAILED!\n\n${err.stack}\n`)
        process.exit(-1)
      } else {
        process.stdout.write('✔\n')
        cb()
      }
    })
  }

  test(name, cb) {
    const test = new Test(name, cb)
    this._queue.push(test)
  }

  /**
   * Run an async test that can return a Promise. If the Promise resolves
   * successfully then the test will pass. If the Promise rejects with an
   * error then the test will be considered failed.
   */
  testAsync(name, action) {
    const test = new Test(name, (cb) => {
      Promise.resolve()
        .then(action)
        .then(() => cb(null), cb)
    })
    this._queue.push(test)
  }
}

process.on('unhandledRejection', (e) => {
  setImmediate(() => {
    console.error('Unhandled promise rejection')
    throw e
  })
})

module.exports = Suite
