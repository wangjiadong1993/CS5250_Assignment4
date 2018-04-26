'''
CS5250 Assignment 4, Scheduling policies simulator
Sample skeleton program
Author: Minh Ho
Input file:
    input.txt
Output files:
    FCFS.txt
    RR.txt
    SRTF.txt
    SJF.txt

Apr 10th Revision 1:
    Update FCFS implementation, fixed the bug when there are idle time slices between processes
    Thanks Huang Lung-Chen for pointing out
Revision 2:
    Change requirement for future_prediction SRTF => future_prediction shortest job first(SJF), the simpler non-preemptive version.
    Let initial guess = 5 time units.
    Thanks Lee Wei Ping for trying and pointing out the difficulty & ambiguity with future_prediction SRTF.
'''
import sys
import heapq

input_file = 'input.txt'

class Process:
    last_scheduled_time = 0
    def __init__(self, id, arrive_time, burst_time):
        self.id = id
        self.arrive_time = arrive_time
        self.burst_time = burst_time
    #for printing purpose
    def __repr__(self):
        return ('[id %d : arrive_time %d,  burst_time %d]'%(self.id, self.arrive_time, self.burst_time))

def FCFS_scheduling(process_list):
    #store the (switching time, proccess_id) pair
    schedule = []
    current_time = 0
    waiting_time = 0
    for process in process_list:
        if(current_time < process.arrive_time):
            current_time = process.arrive_time
        schedule.append((current_time,process.id))
        waiting_time = waiting_time + (current_time - process.arrive_time)
        current_time = current_time + process.burst_time
    average_waiting_time = waiting_time/float(len(process_list))
    return schedule, average_waiting_time

#Input: process_list, time_quantum (Positive Integer)
#Output_1 : Schedule list contains pairs of (time_stamp, proccess_id) indicating the time switching to that proccess_id
#Output_2 : Average Waiting Time
def RR_scheduling(process_list, time_quantum):
    schedule = []
    current_time = 0
    waiting_time = 0
    scheduled_set = [];
    last_preempt_time = dict();
    process_list_len = len(process_list);


    while len(scheduled_set) != 0 or len(process_list) != 0:
        # print '[%s]' % ', '.join(map(str, scheduled_set));
        #get arrived tasks
        if len(process_list) > 0:
        # and process_list[0].arrive_time <= current_time:
            processes = list(filter(lambda x: x.arrive_time <= current_time, process_list));
            process_list = list(filter(lambda x: x.arrive_time > current_time, process_list));
            scheduled_set = processes + scheduled_set;
            for p in processes:
                last_preempt_time[p.id] = p.arrive_time;
            # if len(processes) > 0:
            #     continue;

        #if no more task, fetch one from list
        if len(scheduled_set) == 0:
            current_time = process_list[0].arrive_time;
            continue;
            # scheduled_set.append(process_list[0]);
            # last_preempt_time[process_list[0].id] = current_time; 

        process = scheduled_set[0];
        if len(scheduled_set) >= 2:
            scheduled_set = scheduled_set[1:];
        else:
            scheduled_set = [];

        waiting_time += current_time - last_preempt_time[process.id];
        schedule.append((current_time, process.id));
        if process.burst_time <= time_quantum:    
            current_time += process.burst_time;   
        else:
            current_time += time_quantum;
            process.burst_time -= time_quantum;
            scheduled_set.append(process);
        last_preempt_time[process.id] = current_time;            

    average_waiting_time = waiting_time/float(process_list_len)
    return schedule, average_waiting_time

    

def SRTF_scheduling(process_list):
    #To place the scheduled work.
    #schedule in this format: (current_time, process["Process"].id)
    schedule = []
    #Current Time start from 0
    current_time = 0
    #Accumulated Waiting Time
    waiting_time = 0
    #Preempted Pending Queue, Ready to be scheduled
    #format: (burst_time, {"Process": p, "preempt_time": p.arrive_time})
    preempted_process_heapq = [];
    #The current running task
    #format: {"Process": process["Process"], "start_time": current_time}
    running_task = None;
    #constant, as the length of all tasks
    process_list_len = len(process_list);

    #As long as there are processes that not scheduled
    #or there are pending tasks inside preempted queue
    #or there is running task
    while (len(process_list) != 0 or len(preempted_process_heapq) != 0 or running_task != None):
        


        #in each iteration:
        #1. check there is newly arrived tasks, if there are, then add them to preempted queue.
        #2. if there is no task running:
        #   2.1 if there is no pending task -> current_time ++
        #   2.2 if there is pending task    -> run the shortest pending task
        #3. if there is running task:
        #   3.1 if current task finishing   -> remove it
        #   3.2 if there is no pending task -> current_time ++
        #   3.3 if there is pending task
        #       3.3.1 if it is shorter than current running -> replace it
        #       3.3.2 if it is longer                       -> current time ++


        #1.
        #find newly arrived processes
        if len(process_list) > 0:
            processes = list(filter(lambda x: x.arrive_time <= current_time, process_list));
            process_list = list(filter(lambda x: x.arrive_time > current_time, process_list));
            for p in processes:
                heapq.heappush(preempted_process_heapq, (p.burst_time, {"Process": p, "preempt_time": p.arrive_time}));  

        # if running_task != None:
        #     print "Time: " + str(current_time) + " Process Length: "+ str(len(process_list)) + " " +  ('[%s]' % ', '.join(map(str, list(map(lambda x: x[1]["Process"].id, preempted_process_heapq))))) + " " + str(running_task["Process"]);
        # else:
        #     print "Time: " + str(current_time) + " Process Length: "+ str(len(process_list)) + " " +  ('[%s]' % ', '.join(map(str, list(map(lambda x: x[1]["Process"].id, preempted_process_heapq)))));
        #2
        #if currently there is no running tasks and no pending tasks
        #just imcrement time
        #2.1.
        if running_task== None and len(preempted_process_heapq) == 0:
            # print "2.1 no task running, no pending"
            current_time += 1;
        
        #2.2
        #if there is no running task but there is pending task
        elif running_task == None:
            # print "2.2 no task running, have pending"
            process = heapq.heappop(preempted_process_heapq)[1];
            running_task = {"Process": process["Process"], "start_time": current_time};
            schedule.append((current_time, process["Process"].id));
            waiting_time += (current_time - process["preempt_time"]);
        
        #3.1
        elif running_task["start_time"] + running_task["Process"].burst_time <= current_time:
            # print "3.1 has completed"
            running_task = None;
        #3.2
        # if there is running task and there is no pending tasks
        elif len(preempted_process_heapq) == 0:
            # print "3.2 has no pending"
            #if the task is not finished, still running
            current_time += 1;
        #3.3
        #if there is pending task
        else:
            # get the shortest task from the heap
            shortest_in_heapq = heapq.heappop(preempted_process_heapq)[1];
            #3.3.1
            #if the new task is shorter
            if shortest_in_heapq["Process"].burst_time < (running_task["Process"].burst_time + running_task["start_time"] - current_time ):
                # print "3.3.1 has shorter pending: shortest: burst_time-" + str(shortest_in_heapq["Process"].burst_time);
                running_task["Process"].burst_time = running_task["Process"].burst_time - (current_time  - running_task["start_time"]);
                heapq.heappush(preempted_process_heapq, (running_task["Process"].burst_time, {"Process": running_task["Process"], "preempt_time": current_time}));
                running_task = {"Process": shortest_in_heapq["Process"], "start_time": current_time};
                schedule.append((current_time, shortest_in_heapq["Process"].id));
                waiting_time += (current_time - shortest_in_heapq["preempt_time"]);
            #3.3.2
            else:
                # print "3.3.2 no shorter pending: shortest: burst_time-" + str(shortest_in_heapq["Process"].burst_time)
                heapq.heappush(preempted_process_heapq, (shortest_in_heapq["Process"].burst_time, shortest_in_heapq));
                current_time += 1;
    average_waiting_time = waiting_time/float(process_list_len);
    return schedule, average_waiting_time



def SJF_scheduling(process_list, alpha):
    schedule = [];
    current_time = 0;
    waiting_time = 0;
    #{id : (predict_time, [])}
    history_record = dict();
    history_record_count = 0;
    process_list_len = len(process_list);

    while len(process_list) != 0 or history_record_count != 0:
        
        if len(process_list) > 0:
            processes = list(filter(lambda x: x.arrive_time <= current_time, process_list));
            process_list = list(filter(lambda x: x.arrive_time > current_time, process_list));
            for p in processes:
                if p.id in history_record.keys():
                    history_record[str(p.id)][1].append(p);
                else:
                    temp = [];
                    temp.append(p);
                    history_record[str(p.id)] = (5, temp);
                history_record_count += 1;


        if history_record_count == 0:
            current_time += 1;
            continue;
        else:
            # for p in history_record.items():
            #     print(p);
            non_empty_history_record_array = list(filter(lambda x: len(x[1][1])>0, history_record.items()));
            min_burst_process = min(non_empty_history_record_array, key = lambda x: x[1][0]);
            new_process = history_record[min_burst_process[0]][1].pop(0);
            schedule.append((current_time, new_process.id));
            # print(new_process)
            new_predict = alpha * history_record[min_burst_process[0]][0] + (1-alpha) * new_process.burst_time;
            history_record[min_burst_process[0]] = (new_predict, history_record[min_burst_process[0]][1]);
            waiting_time += (current_time -  new_process.arrive_time);
            current_time += new_process.burst_time;
            history_record_count -= 1;

    average_waiting_time = waiting_time/float(process_list_len);
    return schedule, average_waiting_time

def read_input():
    result = []
    with open(input_file) as f:
        for line in f:
            array = line.split()
            if (len(array)!= 3):
                print ("wrong input format")
                exit()
            result.append(Process(int(array[0]),int(array[1]),int(array[2])))
    return result

def write_output(file_name, schedule, avg_waiting_time):
    with open(file_name,'w') as f:
        for item in schedule:
            f.write(str(item) + '\n')
        f.write('average waiting time %.2f \n'%(avg_waiting_time))


def main(argv):
    process_list = read_input()
    print ("printing input ----")
    for process in process_list:
        print (process)
    print ("simulating FCFS ----")


    FCFS_schedule, FCFS_avg_waiting_time =  FCFS_scheduling(process_list)
    write_output('FCFS.txt', FCFS_schedule, FCFS_avg_waiting_time )
    print ("simulating RR ----")
    rr_process = [];
    for p in process_list:
        rr_process.append(Process(p.id, p.arrive_time, p.burst_time));
    RR_schedule, RR_avg_waiting_time =  RR_scheduling(rr_process,time_quantum = 2)
    write_output('RR.txt', RR_schedule, RR_avg_waiting_time )
    print ("simulating SRTF ----")
    srtf_process = [];
    for p in process_list:
        srtf_process.append(Process(p.id, p.arrive_time, p.burst_time));
    SRTF_schedule, SRTF_avg_waiting_time =  SRTF_scheduling(srtf_process)
    write_output('SRTF.txt', SRTF_schedule, SRTF_avg_waiting_time )
    print ("simulating SJF ----")
    sjf_process = [];
    for p in process_list:
        sjf_process.append(Process(p.id, p.arrive_time, p.burst_time));
    SJF_schedule, SJF_avg_waiting_time =  SJF_scheduling(sjf_process, alpha = 0.5)
    write_output('SJF.txt', SJF_schedule, SJF_avg_waiting_time )

if __name__ == '__main__':
    main(sys.argv[1:])
