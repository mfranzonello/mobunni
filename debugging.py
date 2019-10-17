from time import process_time

open_results = True

# timer for performance evalution
class StopWatch:
    t = {}
    verbose = False

    def timer(event):
        tick = process_time()

        if event not in StopWatch.t:
            StopWatch.t[event] = [{}]
            
        if StopWatch.t[event][-1].get('end') is not None:
            StopWatch.t[event].append({})

        if StopWatch.t[event][-1].get('start') is None:
            StopWatch.t[event][-1]['start'] = tick
        else:
            StopWatch.t[event][-1]['end'] = tick

        tock = StopWatch.t[event][-1].get('end')
           
        if StopWatch.verbose and (tock is not None):
            print(StopWatch.print_time(event))
        
    def print_time(event):
        if event in StopWatch.t:
            event_t = StopWatch.t[event][-1]
            tock = StopWatch.t[event][-1].get('start')
            tick = StopWatch.t[event][-1].get('end')
            if (tick is not None) and (tock is not None):
                print('Time to {}: {:0.3f}s'.format(event, tick - tock))

    def show_results():
        for event in StopWatch.t:
            StopWatch.print_time(event)