    package main.java;

    import backtype.storm.generated.Bolt;
    import backtype.storm.generated.SpoutSpec;
    import backtype.storm.generated.StormTopology;
    import backtype.storm.scheduler.*;

    import java.util.*;

    /**
     * Created by anshushukla on 13/03/16.
     */
    public class UtilityFunction {

        public static Map<ExecutorDetails, WorkerSlot> getCurrentExectoSlotMapping(Cluster cluster, String topoID)
        {
            SchedulerAssignment sa =  cluster.getAssignmentById(topoID);

            Map<ExecutorDetails,WorkerSlot> execToslotMapping  = new HashMap<ExecutorDetails, WorkerSlot>();
            System.out.println("SchedulerAssignment-"+sa);
            if(sa!=null) {
                execToslotMapping = sa.getExecutorToSlot();
                System.out.println("Current state Mapping--\n\n");
                for (ExecutorDetails key : execToslotMapping.keySet()) {
                    System.out.println(key + " - " + execToslotMapping.get(key).getPort());

                }
            }


    //        Map<WorkerSlot,List<ExecutorDetails>>  slotToexecMapping  = new HashMap<>();
    //        for (ExecutorDetails key : execToslotMapping.keySet()) {
    //
    //            WorkerSlot w=execToslotMapping.get(key);
    //            System.out.println(key + " - " + w);
    //            if(slotToexecMapping.containsKey(w)){
    //                //key exists so append
    //                System.out.println("value already entered");
    //
    //                slotToexecMapping.put(w,)
    //            }else{
    //                //key not exists
    //                slotToexecMapping.put(w,);
    //
    //            }
    //        }

            return execToslotMapping;
        }

        public static Map<ExecutorDetails, WorkerSlot> removeSpout_CurrentExectoSlotMapping(List<ExecutorDetails> _spout_executors,Map<ExecutorDetails,WorkerSlot> _execToslotMapping){
            System.out.println("Before removal _execToslotMapping-"+_execToslotMapping);
            for (ExecutorDetails e:_spout_executors)
            {
                _execToslotMapping.remove(e);
            }
            System.out.println("After removal _execToslotMapping-"+_execToslotMapping);
            return _execToslotMapping;
        }

        public static void putExecListToboltnameMapping(String boltName, List<ExecutorDetails> executors, Map<ExecutorDetails, String> execToboltNameMapping){

            System.out.println("putExecListToboltnamemapping----");
            if(executors!=null){
                for (ExecutorDetails e:executors){
                    execToboltNameMapping.put(e,boltName);
                    System.out.println(e+"-"+boltName);
                }
            }
        }

//just for reading no action
        public static void getExecListToboltnameMapping(Map<ExecutorDetails, String> execToboltNameMapping){
            for (ExecutorDetails key : execToboltNameMapping.keySet()) {
                System.out.println(key + " - " + execToboltNameMapping.get(key));

            }
        }

        public static int[][] joinExecToboltNameAndgetCurrentExectoSlotmapping(Map<ExecutorDetails, WorkerSlot> execToslotMapping, Map<ExecutorDetails, String> execToboltNameMapping, Map<String, Integer> test_boltname_NumberPair, Map<WorkerSlot, Integer> test_workeSlot_NumberPair)
        {

            //make list of list here
            Map<WorkerSlot,String> slotToBoltNameMapping  = new HashMap<WorkerSlot,String>();
            System.out.println("Current state JOINED Mapping--\n\n");
            //get list of ports as rows
            //get list of boltnames as columns
            //entry is size of exec_list from getCurrentExectoSlotMapping output


            String boltName=null;
            WorkerSlot w=null;

            Set<WorkerSlot> test_SlotSet = new HashSet<WorkerSlot>();
                for (ExecutorDetails key : execToslotMapping.keySet()) {
                    test_SlotSet.add(execToslotMapping.get(key));
                }
            test_workeSlot_NumberPair.putAll(UtilityFunction.WorkerSlotsetToSortedIndexedList(test_SlotSet));

            Set<String> test_NameSet = new HashSet<String>();
                for (ExecutorDetails key : execToboltNameMapping.keySet()) {
                    test_NameSet.add(execToboltNameMapping.get(key));
                }
            test_boltname_NumberPair.putAll(UtilityFunction.StringsetToSortedIndexedList(test_NameSet));



//            System.out.println("UtilityFunction_workeSlot_NumberPair-"+test_workeSlot_NumberPair);
//            System.out.println("UtilityFunction_boltname_NumberPair-"+test_boltname_NumberPair);



            int[][] CurrentexecToboltNameMatrix=new int[test_workeSlot_NumberPair.size()][test_boltname_NumberPair.size()];
                    for (ExecutorDetails exec : execToslotMapping.keySet()) {

                        WorkerSlot workerSlot = execToslotMapping.get(exec);
                        String s = execToboltNameMapping.get(exec);
                        int entry=exec.getEndTask()-exec.getStartTask()+1;
//                        System.out.println("workerSlot-"+workerSlot);
                        int row_number=test_workeSlot_NumberPair.get(workerSlot);
//                        System.out.println("Exception-"+s+"exec"+exec);
                        if(s!=null) {
                            int column_number = test_boltname_NumberPair.get(s);
//                        execToboltNameMatrix[row_number][column_number]+=entry;
                            CurrentexecToboltNameMatrix[row_number][column_number] += 1;
                            System.out.println(exec + " - " + workerSlot.getPort() + "-Boltname-" + s + "-row-" + row_number + "-column-" + column_number);
                        }
    //                    if(execToslotMapping.get(exec).getPort()==i){
    //                        String _bname=execToboltNameMapping.get(exec);
    //                        int entry=exec.getEndTask()-exec.getStartTask()+1;
    //                        execToboltNameMatrix[i][_boltname_NumberPair.get(_bname)]=entry;
    //                        System.out.println("i-"+i+"-_boltname_NumberPair.get(_bname)-"+_boltname_NumberPair.get(_bname)+"-entry-"+entry);
                        }
            System.out.println("printing a current 2-D array-"+Arrays.deepToString(CurrentexecToboltNameMatrix));


            return CurrentexecToboltNameMatrix;

            }



        public  static int setFlagforScheduling(TopologyDetails t_name, Cluster cluster, String jsonfilepath, int needsSchedulingFlagBolt, int needsSchedulingFlagSpout, Map<String, String> vm_Name_supIDMap){

//            for (TopologyDetails t_name : topologyDetails) {
                StormTopology topology = t_name.getTopology();
                String topoName = t_name.getName();
                System.out.println("\n\n\t\t\t\t--Checking topo needs scheduling---" + topoName);

                //idea2:--- New CODE Checking topo needs scheduling --

                Map<String, Bolt> bolts = topology.get_bolts();
                Map<String, SpoutSpec> spouts = topology.get_spouts();
                List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();


                for (String boltName : bolts.keySet()) {
                    executors = cluster.getNeedsSchedulingComponentToExecutors(t_name).get(boltName);
                    String thread_count_in_conf = JsonFIleReader.getJsonThreadCount(jsonfilepath, topoName, boltName);
                    System.out.println("executors within bolt- " + boltName + "-" + executors);

//                    StateFromConf.createSetFromConf(jsonfilepath, topoName, boltName, vm_Name_supIDMap, boltName_Set_FromConf, workerslot_Set_FromConf, fullMappingRes_conf);//state from conf

                    if (executors != null) {

                        //while doing rebalance/new submission thread_count_in_conf = = executor.size for that bolt
                        System.out.println("CHECKING: Rebalance/New submission thread_count_in_conf = = executor.size for that bolt");
                        System.out.println("\t\texecutor list size--" + executors.size() + "-thread_count_in_conf-" + thread_count_in_conf);
                        if (executors.size() != Integer.parseInt(thread_count_in_conf)) {//checking conf_thread --> (topo_exec passed/Rebalance_exec_passed)
                            System.out.println("\n\n\t\t\t\t****FAILED/UNEQUAL:EITHER some executors have failed OR Conf entry is wrong For-" + topoName + "---" + boltName + "******");
                            System.out.println("\n\n\t\t\t\t**No logic to handle Re-Scheduling----\n\n\t\tEXITING***");
                            //No logic to handle Re-Scheduling
                            needsSchedulingFlagBolt = 0;
                            break;
                        } else {
                            needsSchedulingFlagBolt = 1;
                            System.out.println("\t\t\t\t*****Rebalance/New submission--Toponame********" + topoName + "****BoltName****" + boltName+"\n\n");
                        }

                    }
                }


                for (String spoutName : spouts.keySet()) {
                    executors = cluster.getNeedsSchedulingComponentToExecutors(t_name).get(spoutName);
                    System.out.println("executors within spout- " + executors);
                    if (executors != null && needsSchedulingFlagBolt == 1)//Flag checking: if condition for all bolts are fine the inly go for spout
                    {
                        needsSchedulingFlagSpout = 1;
                        System.out.println("\t\t\t\t*****Rebalance/New submission--Toponame********" + topoName + "****SpoutName****" + spoutName+"\n\n");
                    }

                }


                System.out.println("\t\tFLAGS:needsSchedulingFlagBolt-" + needsSchedulingFlagBolt + "-needsSchedulingFlagSpout-" + needsSchedulingFlagSpout+"\n");
//            }
            if(needsSchedulingFlagBolt==1 || needsSchedulingFlagSpout==1)
                return 1;
            else return 0;


        }




        public static Map<WorkerSlot, Integer> WorkerSlotsetToSortedIndexedList(Set<WorkerSlot> _test_SlotSet){
            List<WorkerSlot> test_Slotlist = new ArrayList<WorkerSlot>(_test_SlotSet);
            Map<WorkerSlot,Integer> _test_workeSlot_NumberPair = new HashMap<>();
            int testcount_workeSlot_NumberPair=0;
//            System.out.println("old list-" + test_Slotlist);
            Collections.sort(test_Slotlist, new Comparator<WorkerSlot>() {
                public int compare(WorkerSlot idx1, WorkerSlot idx2) {
                    return Double.compare(idx1.getPort(), idx2.getPort());
                }
            });
//            System.out.println("sorted list-" + test_Slotlist);
            for (WorkerSlot wk : test_Slotlist) {
                _test_workeSlot_NumberPair.put(wk, testcount_workeSlot_NumberPair);
                testcount_workeSlot_NumberPair+=1;
            }
            return _test_workeSlot_NumberPair;
        }


        public static Map<String, Integer> WorkerSlotStringsetToSortedIndexedList(Set<String> _test_SlotSet){
            List<String> test_Slotlist = new ArrayList<String>(_test_SlotSet);
            Map<String,Integer> _test_workeSlot_NumberPair = new HashMap<>();
            int testcount_workeSlot_NumberPair=0;
//            System.out.println("old list-" + test_Slotlist);
            Collections.sort(test_Slotlist, new Comparator<String>() {
                public int compare(String idx1, String idx2) {
                    Double d1=Double.parseDouble(idx1.split(":")[1]);
                    Double d2=Double.parseDouble(idx2.split(":")[1]);
                    return Double.compare(d1,d2);
                }
            });
//            System.out.println("sorted list-" + test_Slotlist);
            for (String wk : test_Slotlist) {
                _test_workeSlot_NumberPair.put(wk, testcount_workeSlot_NumberPair);
                testcount_workeSlot_NumberPair+=1;
            }
            return _test_workeSlot_NumberPair;
        }

        public static HashMap<String, Integer> StringsetToSortedIndexedList(Set<String> _test_NameSet) {
            List<String> test_Namelist = new ArrayList<String>(_test_NameSet);
            HashMap<String, Integer> _test_boltname_NumberPair = new HashMap<>();
            int testcount=0;
//            System.out.println("old name list-" + test_Namelist);
            Collections.sort(test_Namelist);
//            System.out.println("sorted name list-" + test_Namelist);
            for (String wk : test_Namelist) {
                _test_boltname_NumberPair.put(wk, testcount);
                testcount+=1;
            }
        return _test_boltname_NumberPair;
        }



        public static Map<ExecutorDetails, String> currentExecListToboltnameDemo(TopologyDetails t, Cluster cluster){
            StormTopology topology = t.getTopology();
            String topoID = t.getId();
            String topoName = t.getName();
            Map<ExecutorDetails, String> execToboltNameMapping = new HashMap<ExecutorDetails, String>();
            Map<String, Bolt> bolts = topology.get_bolts();
            Map<String, SpoutSpec> spouts = topology.get_spouts();
            int checkNullflag=0;

            List<ExecutorDetails> bolt_executors = new ArrayList<ExecutorDetails>();
            List<ExecutorDetails> spout_executors = new ArrayList<>();


            for (String boltName : bolts.keySet()) {
                //get key and value in same loop
                bolt_executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(boltName);
                System.out.println("executors within this bolt- " + bolt_executors);
                Bolt bolt = bolts.get(boltName);
                System.out.println("bolt_executors-"+bolt_executors);
                if(bolt_executors!=null) {
                    UtilityFunction.putExecListToboltnameMapping(boltName, bolt_executors, execToboltNameMapping);//check for null first
                }
            }


            for (String spoutName : spouts.keySet()) {
                SpoutSpec spout = spouts.get(spoutName);
                spout_executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(spoutName);
                System.out.println("spout_executors-"+spout_executors);
                if(spout_executors != null) {
                    UtilityFunction.putExecListToboltnameMapping(spoutName, spout_executors, execToboltNameMapping);//check for null first
                }
            }

            System.out.println("\n\ngetting putExecListToboltnamemapping-");
            UtilityFunction.getExecListToboltnameMapping(execToboltNameMapping);

            System.out.println("execToboltNameMapping size-"+execToboltNameMapping.size());
            return execToboltNameMapping;
        }


//        public static void getCurrentExectoSlotmapping()
//        {
//
//        }

        public static int[][] createCurrentMatrixDemo(TopologyDetails t, Cluster cluster, Map<String, Integer> test_boltname_NumberPair, Map<WorkerSlot, Integer> test_workeSlot_NumberPair, int row_size_fromConf, int column_size_fromConf, Map<ExecutorDetails, String> executorDetailsStringMap){

            String topoID = t.getId();

            Map<ExecutorDetails, WorkerSlot> execToslotMapping = new HashMap<ExecutorDetails, WorkerSlot>();
            execToslotMapping = UtilityFunction.getCurrentExectoSlotMapping(cluster, topoID);//includes spout also

            int[][] CurrentexecToboltNameMatrix=new int[row_size_fromConf][column_size_fromConf];
            System.out.println("CurrentexecToboltNameMatrix-"+Arrays.deepToString(CurrentexecToboltNameMatrix));

            System.out.println("Before calling Join Utility function arg passed -" + execToslotMapping);
            if(execToslotMapping.size()!=0 && execToslotMapping!=null) {
                CurrentexecToboltNameMatrix = UtilityFunction.joinExecToboltNameAndgetCurrentExectoSlotmapping(execToslotMapping, executorDetailsStringMap, test_boltname_NumberPair, test_workeSlot_NumberPair);
            }
            else{
                System.out.println("UtilityFunction.joinExecToboltNameAndgetCurrentExectoSlotmapping  is not called");
            }
        return  CurrentexecToboltNameMatrix;

        }


    }
