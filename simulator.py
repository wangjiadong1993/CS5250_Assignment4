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

    # while(True):
    #     # id there is no more processes to schedule
    #     if len(process_list) == 0:
    #         break;
    #     #iterate through all the remaining processes
        
    #     for process in process_list:
    #         isFirst = True;
    #         #when reaching a not-yet-arrived process
    #         #break
    #         if current_time < process.arrive_time and len(scheduled_set) == 0:
    #             if isFirst:
    #                 current_time = process.arrive_time;
    #             break;
    #         #for each current-processing processes
    #         else:
    #             if not (process in scheduled_set):
    #                 waiting_time += current_time - process.arrive_time;
    #                 scheduled_set.append(process);
    #             else:
    #                 waiting_time += current_time - last_preempt_time[process.id];
    #             schedule.append((current_time, process.id));
    #             if process.burst_time <= time_quantum:
    #                 current_time += process.burst_time;
    #                 last_preempt_time[process.id] = current_time;
    #                 process_list.remove(process);
    #             else:
    #                 current_time += time_quantum;
    #                 last_preempt_time[process.id] = current_time;
    #                 process.burst_time -= time_quantum;
    #         isFirst = False;
     
    while len(scheduled_set) != 0 or len(process_list) != 0:
        # print '[%s]' % ', '.join(map(str, scheduled_set));
        #get arrived tasks
        if len(process_list) > 0:
        # and process_list[0].arrive_time <= current_time:
            processes = list(filter(lambda x: x.arrive_time <= current_time, process_list));
            process_list = list(filter(lambda x: x.arrive_time > current_time, process_list));
            scheduled_set = processes + scheduled_set;
            for p in processes:
                last_preempt_time[p.id] = current_time;
            if len(processes) > 0:
                continue;

        #if no more task, fetch one from list
        if len(scheduled_set) == 0:
            current_time = process_list[0].arrive_time;
            scheduled_set.append(process_list[0]);
            last_preempt_time[process_list[0].id] = current_time; 

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
    schedule = []
    current_time = 0
    waiting_time = 0
    preempted_process_heapq = [];
    running_task = None;

    while len(process_list) != 0 or len(preempted_process_heapq) != 0:

        if current_time >= process_list[0].arrive_time:
            process = process_list[0];
            process_list = process_list[1:];
            heapq.heappush(preempted_process_heapq, (process.id, {"Process": process, "preempt_time": current_time}));
            if running_task == None:
                process = heapq.heappop(preempted_process_heapq)[1];
                running_task = {"Process": process["Process"], "start_time": current_time};
                schedule.append((current_time, process["Process"].id));
            else:
                shortest_in_heapq = heapq.heappop(preempted_process_heapq)[1];
                if shortest_in_heapq["Process"].burst_time < (running_task["Process"].burst_time - running_task["start_time"] - current_time) :
                    heapq.heappush(preempted_process_heapq, (unning_task["Process"].id, {"Process": running_task["Process"], "preempt_time": current_time}));
                    running_task = {"Process": shortest_in_heapq["Process"], "start_time": current_time};
                else:
                    heapq.heappush(preempted_process_heapq, (shortest_in_heapq["Process"].id, shortest_in_heapq));
        elif current_time >= running_task["start_time"] + running_task["Process"].burst_time:
            running_task = None;
            if len(preempted_process_heapq) == 0:
                pass;
            else:
                process = heapq.heappop(preempted_process_heapq)[1];
                running_task = {"Process": process["Process"], "start_time": current_time};
                schedule.append((current_time, process["Process"].id));
        else:
            current_time = process_list[0].arrive_time;
            if running_task != None:
                current_time = min(current_time, running_task["start_time"] + running_task["Process"].burst_time);

    while(True):

        if current_time >= running_task["start_time"] + running_task["Process"].burst_time:
            running_task = None;
            if len(preempted_process_heapq) == 0:
                pass;
            else:
                process = heapq.heappop(preempted_process_heapq)[1];
                running_task = {"Process": process["Process"], "start_time": current_time};
                schedule.append((current_time, process["Process"].id));
        else:
            if running_task != None:
                current_time = min(current_time, running_task["start_time"] + running_task["Process"].burst_time);




def SJF_scheduling(process_list, alpha):
    schedule = [];
    current_time = 0;
    waiting_time = 0;
    history_record = dict();


    while True:
        if len(process_list) == 0:
            break;

        process_arrived = list(filter(lambda x: x.arrive_time <= current_time, process_list));

        if len(process_arrived) == 0:
            current_time = process_list[0].arrive_time;
        else:
            predicted_processes = list(map(lambda x: {"predict": get_predict(x, history_record), "process": process} , process_arrived));
            min_predicted_burst  = min(list(map(lambda x: x["predict"], predicted_processes)));
            predicted_shortest_processes = list(filter(lambda x: x["predict"] == min_predicted_burst, predicted_processes));
            first_predicted_arrival = min(list(map(lambda x: x["process"].arrive_time, predicted_shortest_processes)));
            predicted_first_shortest_process = list(filter(lambda x: x["process"].arrive_time == first_predicted_arrival, predicted_shortest_processes))[0];
            schedule.append((current_time, predicted_first_shortest_process["process"].id));
            current_time += predicted_first_shortest_process["Process"].burst_time;
            process_list.remove(predicted_first_shortest_process["process"]);
            history_record[predicted_first_shortest_process["process"].id] = {"predict": predicted_first_shortest_process["predict"], "actual": predicted_first_shortest_process["process"].burst_time};


def get_predict(process, history_record, alpha):
    if process.id in history_record.keys():
        return alpha * history_record[process.id]["predict"] + (1-alpha) * history_record[process.id]["actual"];
    else:
        return 5;




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
    RR_schedule, RR_avg_waiting_time =  RR_scheduling(process_list,time_quantum = 2)
    write_output('RR.txt', RR_schedule, RR_avg_waiting_time )
    print ("simulating SRTF ----")
    SRTF_schedule, SRTF_avg_waiting_time =  SRTF_scheduling(process_list)
    write_output('SRTF.txt', SRTF_schedule, SRTF_avg_waiting_time )
    print ("simulating SJF ----")
    SJF_schedule, SJF_avg_waiting_time =  SJF_scheduling(process_list, alpha = 0.5)
    write_output('SJF.txt', SJF_schedule, SJF_avg_waiting_time )

if __name__ == '__main__':
    main(sys.argv[1:])
