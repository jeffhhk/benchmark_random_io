#lang racket

#|
Puts a constant amount of work into each of n threads, for various
values of n.

Example usage:

    racket test_random_reads_thread_parallel.rkt .../test_randio

Example result:
    '(((num-threads 1) (num-reads-per-thread 10000) (dt 1651)) ((num-threads 3) (num-reads-per-thread 10000) (dt 4666)) ((num-threads 6) (num-reads-per-thread 10000) (dt 9313)) ((num-threads 10) (num-reads-per-thread 10000) (dt 15479)) ((num-threads 19) (num-reads-per-thread 10000) (dt 29404)))

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


(define (run-in-parallel-threads n f)
  (let* ((t0 (current-milliseconds))
         (ths (map (lambda (junk) (thread f))
                   (stream->list (in-range 0 n))))
         (evs (map thread-dead-evt ths)))
    (let loop ((i n))
      (if (<= i 0)
          #f
          (begin
            (apply sync evs)
            (printf "thread quit after ~a\n" (- (current-milliseconds) t0))
            (loop (- i 1)))))))


(define (run-each-benchmark ilist)
  ;; TODO: use the rough estimate to calibrate how much work
  ;; is run in each parallel thread
  ;; TODO: compute combined reads/sec
  (for/list ((i ilist))
    (let* ((t0 (current-milliseconds))
           (_ (printf "about to run i=~a\n" i))
           (_ (run-in-parallel-threads i rough-estimate-dt-1-thread))
           (dt (- (current-milliseconds) t0))
           (total-reads (* i num-iter-rough))
           (reads-per-sec (exact-floor (/ total-reads (/ dt 1000))))
           (_ (sleep 1)))
      `((num-threads ,i)
        (num-reads-per-thread ,num-iter-rough)
        (reads-per-sec ,reads-per-sec)
        (dt ,dt)))))


(parameterize ((afile-big (vector-ref (current-command-line-arguments) 0)))
  (printf "about to test ~a\n" (afile-big))
  (run-each-benchmark (exp-spaced 5 20)))

