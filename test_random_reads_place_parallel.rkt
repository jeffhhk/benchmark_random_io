#lang racket
(require racket/place)

#|
Puts a constant amount of work into each of n threads, for various
values of n.

Example usage:

    racket test_random_reads_place_parallel.rkt .../test_randio

Example result:


|#

(define (random-big m n)
  (define bits 31)
  (define base (expt 2 bits))
  (define (random-iter d)
    (if (<= d base)
        (random 0 d)
        (+
         (random 0 base)
         (arithmetic-shift
          (random-iter (arithmetic-shift d (- bits)))
          bits))))
  (+ m (random-iter (- n m))))


(define (test-file-random-read afile n)
  (define i-max (file-size afile))
  (define f (open-input-file afile))
  (let loop ((i n))
    (when (>= i 0)
      (let ((pos (random-big 0 i-max)))
        (file-position f pos)
        (read-bytes 1 f)
        (loop (- i 1)))))
  (close-input-port f))

(define afile-big (make-parameter #f))

(define num-iter-rough 10000)

(define (rough-estimate-dt-1-thread)
  (let* ((t0 (current-milliseconds))
         (_ (test-file-random-read (afile-big) num-iter-rough))
         (dt (- (current-milliseconds) t0)))
    (exact->inexact (/ dt num-iter-rough 1000))))


;; Note that the final element is always x-max, but the first element
;; is not guaranteed to be 1.  For n=10, it's not clear how to have
;; both unless one is willing to tolerate duplicate 1 values.
(define (exp-spaced n x-max)
  (define (f i)
    (exact-floor
     (exp
      (/ (* i (log x-max))
         n))))
  (let loop ((i 1))
    (if (> i n)
        '()
        (cons (f i)
              (loop (+ i 1))))))

;;(exp-spaced 10 40)
;; '(1 2 3 4 6 9 13 19 27 40)


(define (run-in-parallel-places n)
  (let* ((t0 (current-milliseconds))
         (_ (printf "about to start places\n"))
         ;; TODO: since we cannot pass arguments to a place via a closure,
         ;; use something else to vary the amount of work ala
         ;; benchmark_random_reads.py.  place channel?  parameterize?
         (ths (map (lambda (junk) (place ch 'rough-estimate-dt-1-thread))
                   (stream->list (in-range 0 n))))
         (_ (printf "waiting for places to finish\n"))
         (evs (map place-dead-evt ths)))
    (let loop ((i n))
      (if (<= i 0)
          #f
          (begin
            (apply sync evs)
            (printf "place quit after ~a\n" (- (current-milliseconds) t0))
            (loop (- i 1)))))))


(define (run-each-benchmark ilist)
  ;; TODO: use the rough estimate to calibrate how much work
  ;; is run in each parallel place
  ;; TODO: compute combined reads/sec
  (printf "will run benchmarks: ~a\n" ilist)
  (for/list ((i ilist))
    (let* ((t0 (current-milliseconds))
           (_ (printf "about to run i=~a\n" i))
           (_ (run-in-parallel-places i))
           (dt (- (current-milliseconds) t0))
           (total-reads (* i num-iter-rough))
           (reads-per-sec (exact-floor (/ total-reads (/ dt 1000))))
           (_ (sleep 1)))
      `((num-threads ,i)
        (num-reads-per-thread ,num-iter-rough)
        (reads-per-sec ,reads-per-sec)
        (dt ,dt)))))

#|
*** Warning ***

The amount of time it takes for the places to quit in this program seems inaccurate.  Compare reads-per-sec for num-threads=1:

test_random_reads_thread_parallel.rkt:
    '(((num-threads 1) (num-reads-per-thread 10000) (reads-per-sec 6422) (dt 1557)) ((num-threads 3) (num-reads-per-thread 10000) (reads-per-sec 6468) (dt 4638)) ((num-threads 6) (num-reads-per-thread 10000) (reads-per-sec 6525) (dt 9194)) ((num-threads 10) (num-reads-per-thread 10000) (reads-per-sec 6482) (dt 15425)) ((num-threads 19) (num-reads-per-thread 10000) (reads-per-sec 6511) (dt 29178)))

test_random_reads_place_parallel.rkt:
    '(((num-threads 1) (num-reads-per-thread 10000) (reads-per-sec 31847) (dt 314)) ((num-threads 3) (num-reads-per-thread 10000) (reads-per-sec 48076) (dt 624)) ((num-threads 6) (num-reads-per-thread 10000) (reads-per-sec 62565) (dt 959)) ((num-threads 10) (num-reads-per-thread 10000) (reads-per-sec 68634) (dt 1457)) ((num-threads 19) (num-reads-per-thread 10000) (reads-per-sec 53490) (dt 3552)))

The machine used in both cases is the same.

6422 is a plausible number for said machine, in keeping with what is measured with benchmark_random_reads.py.  31847 is not a plausible number.

|#


;; Without the module+, each place will erroneously start reevaluating
;; the program from the beginning, instead of calling rough-estimate-dt-1-thread
(module+ main
  (parameterize ((afile-big (vector-ref (current-command-line-arguments) 0)))
    (printf "about to test ~a\n" (afile-big))
    (run-each-benchmark (exp-spaced 5 20))))

