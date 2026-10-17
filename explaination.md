Memory leaked cuz a default list arg in the fold function kept saving everything instead of resetting.
Threading had race conditions on the ID set so it randomly dropped data.
Missed the epoch shift rule where multiples of 7 mean nanoseconds so those lines just crashed.
