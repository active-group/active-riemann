#!/bin/sh

bench () {
  SETUP=$1
  echo "$SETUP START" `date`
  lein bench "$SETUP"
  echo "$SETUP DONE" `date`
}

NUMBER_EVENTS=3000
KEY_VALUES=30
bench "{:task :backup  :number-of-events $NUMBER_EVENTS :number-of-event-key-values $KEY_VALUES}"
bench "{:task :restore :number-of-events $NUMBER_EVENTS :number-of-event-key-values $KEY_VALUES :f-insertion? :dummy}"
bench "{:task :restore :number-of-events $NUMBER_EVENTS :number-of-event-key-values $KEY_VALUES :f-insertion? :util}"
bench "{:task :restore :number-of-events $NUMBER_EVENTS :number-of-event-key-values $KEY_VALUES :f-insertion? :enhanced}"

