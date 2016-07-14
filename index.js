/**
 * @author Dmitriy Bizyaev
 */

'use strict';

/**
 *
 * @ignore
 */
const co = require('co'),
    prettyMs = require('pretty-ms');

/**
 *
 * @param {number} msecs
 * @returns {Promise}
 */
exports.wait = msecs => new Promise(resolve => void setTimeout(resolve, msecs));

/**
 *
 * @param {Promise} promise
 * @param {number} timeout
 * @returns {Promise}
 */
exports.withTimeout = (promise, timeout) => new Promise((resolve, reject) => {
    let settled = false;

    const cb = () => {
        settled = true;
        reject(new Error('Operation timed out'));
    };

    const to = setTimeout(cb, timeout);

    promise
        .then(res => {
            clearTimeout(to);
            if (!settled) resolve(res);
        })
        .catch(err => {
            clearTimeout(to);
            if (!settled) reject(err);
        });
});

/**
 * @typedef {Object} AsyncArrayProcessOptions
 * @property {number} threads
 */

/**
 *
 * @const {AsyncArrayProcessOptions}
 */
const defaultAsyncArrayProcessOptions = {
    threads: 1
};

/**
 *
 * @param {*[]} array
 * @param {function(item: *): Promise} fn
 * @param {AsyncArrayProcessOptions} options
 * @returns {Promise}
 */
exports.asyncMap = (array, fn, options) => co(function* () {
    options = Object.assign({}, defaultAsyncArrayProcessOptions, options);

    const itemsNum = array.length,
        threads = Math.min(itemsNum, options.threads),
        ret = new Array(itemsNum);

    let processed = 0,
        error = false;

    const doWork = () => co(function* () {
        while (processed < itemsNum) {
            if (error) break;

            const i = processed;
            processed++;

            ret[i] = yield fn(array[i]);
        }
    });

    const workPromises = [];

    for (let j = 0; j < threads; j++) workPromises.push(doWork());

    try {
        yield workPromises;
    }
    catch (err) {
        error = true;
        throw err;
    }

    return ret;
});

/**
 *
 * @param {*[]} array
 * @param {function(item: *): Promise} fn
 * @param {AsyncArrayProcessOptions} options
 * @returns {Promise}
 */
exports.asyncForEach = (array, fn, options) => co(function* () {
    options = Object.assign({}, defaultAsyncArrayProcessOptions, options);

    const itemsNum = array.length,
        threads = Math.min(itemsNum, options.threads);

    let processed = 0,
        error = false;

    const doWork = () => co(function* () {
        while (processed < itemsNum) {
            if (error) break;

            const i = processed;
            processed++;

            yield fn(array[i]);
        }
    });

    const workPromises = [];

    for (let j = 0; j < threads; j++) workPromises.push(doWork());

    try {
        yield workPromises;
    }
    catch (err) {
        error = true;
        throw err;
    }
});

const isNat = x => typeof x === 'number' && !isNaN(x) && x % 1 === 0 && x > 0;

/**
 * @typedef {Object} WorkerOptions
 * @property {number} [queueLength]
 */

/**
 * @class {Worker}
 */
exports.Worker = class Worker {
    /**
     *
     * @param {function(item: *): Promise|*} processItemFn
     * @param {number} threads
     * @param {WorkerOptions} [options]
     */
    constructor(processItemFn, threads, options) {
        if (typeof processItemFn !== 'function')
            throw new Error('processItemFn must be a function');
        
        if (!isNat(threads))
            throw new Error('threads must be a positive integer');
        
        options = options || {};

        this._processItemFn = processItemFn;
        this._threads = threads;
        this._queueLength = options.queueLength || 0;
        this._queue = [];
        this._threadsActive = 0;
        this._itemCallbacks = new Map();
    }

    /**
     *
     * @param {*} item
     */
    addItem(item) {
        if (this._queue.length >= this._queueLength) return false;
        
        this._queue.push(item);
        if (this._threadsActive < this._threads) this._doWork();
        return true;
    }

    /**
     * 
     * @param {*} item
     * @returns {Promise}
     */
    processItem(item) {
        if (this._queue.length >= this._queueLength)
            return Promise.reject(new Error('Queue is full'));

        this._queue.push(item);
        
        const promise = new Promise((resolve, reject) =>
            void this._itemCallbacks.set(item, { resolve, reject }));

        if (this._threadsActive < this._threads) this._doWork();
        return promise;
    }

    /**
     *
     * @private
     */
    _doWork() {
        const that = this;

        this._threadsActive++;

        co(function* () {
            let currentItem = null;

            while (currentItem = that._queue.shift()) {
                const callbacks = that._itemCallbacks.get(currentItem);
                that._itemCallbacks.delete(currentItem);
                
                try {
                    const res = yield Promise.resolve(that._processItemFn(currentItem));
                    if (callbacks) callbacks.resolve(res);
                }
                catch (err) {
                    if (callbacks) callbacks.reject(err);
                }
            }

            that._threadsActive--;
        });
    }
};

/**
 *
 * @const {number}
 */
const DEFAULT_TOO_LONG_TIME = 1000 * 60 * 10; // 10 minutes

/**
 *
 * @const {number}
 */
const DEFAULT_WARN_INTERVAL = 1000 * 60; // One minute

/**
 * @typedef {Object} SelfServiceWorkerOptions
 * @property {number} [threads]
 * @property {function(item: *, err: Error)} [onError]
 * @property {number} [tooLongTime]
 * @property {number} [tooLongWarnInterval]
 * @property {Object} [logger]
 */

/**
 *
 * @const {SelfServiceWorkerOptions}
 */
const defaultSelfServiceWorkerOptions = {
    threads: 1,
    logger: null,
    onError: null,
    tooLongTime: DEFAULT_TOO_LONG_TIME,
    tooLongWarnInterval: DEFAULT_WARN_INTERVAL
};

/**
 *
 * @type {number}
 */
let nextSelfServiceWorkerId = 0;

/**
 *
 * @class SelfServiceWorker
 */
exports.SelfServiceWorker = class SelfServiceWorker {
    /**
     *
     * @param {function(item: *): Promise|*} processItemFn
     * @param {function(number): Promise} getNextItemFn
     * @param {SelfServiceWorkerOptions} [options]
     */
    constructor(processItemFn, getNextItemFn, options) {
        options = Object.assign({}, defaultSelfServiceWorkerOptions, options);

        this._id = nextSelfServiceWorkerId++;
        this._processItemFn = processItemFn;
        this._getItemFn = getNextItemFn;
        this._onError = options.onError || null;
        this._threads = options.threads || 1;
        this._tooLongTime = options.tooLongTime;
        this._tooLongWarnInterval = options.tooLongWarnInterval;
        this._threadsActive = 0;
        this._logger = options.logger || null;

        this._logger && this._logger.verbose(
            `SelfServiceWorker ${ this._id } created: ${ this._threads } threads max`
        );
    }

    /**
     *
     */
    start() {
        this._logger && this._logger.verbose(
            `SelfServiceWorker ${ this._id }: start() call ` +
            `when ${ this._threadsActive }/${ this._threads } threads are active`
        );

        while (this._threadsActive < this._threads) {
            this._threadsActive++;
            this._doWork(this._threadsActive - 1);
        }
    }

    _warnTooLong(threadId, timeStart) {
        const time = Date.now() - timeStart;

        this._logger && this._logger.warn(
            `SelfServiceWorker ${ this._id } thread ${ threadId }: ` +
            `processing is taking too long (${ prettyMs(time) })`
        );
    }

    _error(threadId, item, err) {
        this._logger && this._logger.error(
            `SelfServiceWorker ${ this._id } thread ${ threadId }: ` +
            `error while processing item: ${ err }`
        );

        this._onError && this._onError(item, err);
    }

    /**
     *
     * @private
     */
    _doWork(threadId) {
        const that = this;

        return co(function* () {
            let currentItem = null,
                itemsProcessed = 0,
                startTime = Date.now(),
                warningInterval = null,
                startWarnTimeout = null;

            startWarnTimeout = setTimeout(() => {
                const warn = () => that._warnTooLong(threadId, startTime);
                warningInterval = setInterval(warn, that._tooLongWarnInterval);
                startWarnTimeout = null;
            }, that._tooLongTime);

            while (currentItem = yield that._getItemFn(threadId)) {
                try {
                    yield that._processItemFn(currentItem, threadId);
                }
                catch (err) {
                    that._error(threadId, currentItem, err);
                }

                itemsProcessed++;
            }

            that._logger && that._logger.verbose(
                `SelfServiceWorker ${ that._id } thread ${ threadId } finished: ` +
                `${ itemsProcessed } items processed ` +
                `in ${ prettyMs(Date.now() - startTime) }`
            );

            if (startWarnTimeout) clearTimeout(startWarnTimeout);
            if (warningInterval) clearInterval(warningInterval);
            that._threadsActive--;
        });
    }
};
