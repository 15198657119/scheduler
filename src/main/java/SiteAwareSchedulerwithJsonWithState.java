
    package main.java;

    import backtype.storm.generated.Bolt;
    import backtype.storm.generated.SpoutSpec;
    import backtype.storm.generated.StormTopology;
    import backtype.storm.scheduler.*;
    import org.json.simple.parser.JSONParser;

    import java.util.*;

    //running 9_3_30 before that
    public class SiteAwareSchedulerwithJsonWithState implements IScheduler {

        private static final String SITE = "site"; //used only while caching supervisor details
    //    private static final String THREADS = "threads";
    String jsonfilepath = "/data/tetc/apache-storm-0.9.4-ForScheduling/conf/inputTopoConfig.json";

        @Override
        public void prepare(Map conf) {
        }

        @Override
        public void schedule(Topologies topologies, Cluster cluster) {
            System.out.println("=======================================TEST:running====================================");
            Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
            Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();  //get supervisor details of cluster
            int needsSchedulingFlagBolt = 0;
            int needsSchedulingFlagSpout = 0;
            int needsSchedulingFlag=0;

            for (TopologyDetails t_name : topologyDetails) {

                needsSchedulingFlag = UtilityFunction.setFlagforScheduling(t_name, cluster, jsonfilepath, needsSchedulingFlagBolt, needsSchedulingFlagSpout);

                if (needsSchedulingFlag == 1) {

                    StateFromConf.setVmNameSupervisorMapping(cluster, SITE);
                    Map<String, SupervisorDetails> supervisors = new HashMap<String, SupervisorDetails>();
                    JSONParser parser = new JSONParser();
                    //logging: getting supervisor names
                    {
                        for (String s : cluster.getSupervisors().keySet()) {
                            System.out.println("supervisor names are -" + s);

                        }
                    }
                    //

                    System.out.println("TEST:caching supervisors indexed by their sites in a hash map...");
                    for (SupervisorDetails s : cluster.getSupervisors().values()) {
//                    System.out.println("TEST:supervisor name-" + s);
                        Map<String, String> metadata = (Map<String, String>) s.getSchedulerMeta();
                        if (metadata.get(SITE) != null) {
                            System.out.println("TEST: checking if metadata is set on this supervisor....");
                            supervisors.put((String) metadata.get(SITE), s);
                            System.out.println("TEST:Value for this supervisor-" + (String) metadata.get(SITE));
                        }
                        System.out.println(s.getAllPorts() + "\n");
                    }

                    Map<String, String> vm_Name_supIDMap = new HashMap<>();
                    vm_Name_supIDMap = StateFromConf.setVmNameSupervisorMapping(cluster, SITE);
                    System.out.println("vm_Name_supIDMap-\n" + vm_Name_supIDMap + "\n");

                    for (TopologyDetails t : topologyDetails) {

                        StormTopology topology = t.getTopology();
                        String topoID = t.getId();
                        String topoName = t.getName();

                        Map<ExecutorDetails, String> execToboltNameMapping = new HashMap<ExecutorDetails, String>();
                        Map<String, Bolt> bolts = topology.get_bolts();
                        Map<String, SpoutSpec> spouts = topology.get_spouts();
                        Set<String> boltName_Set_FromConf = new HashSet<>();
                        Set<String> workerslot_Set_FromConf = new HashSet<>();
                        List<String> FullMappingRes_conf = new ArrayList();
                        String site = null;
                        String slotID = null;
                        String threadCount = null;
                        Map<String, List<ExecutorDetails>> vmSlotExecMapping = new HashMap<String, List<ExecutorDetails>>();
                        for (String boltName : bolts.keySet()) {//get key and value in same loop
                            List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();
                            Bolt bolt = bolts.get(boltName);
                            System.out.println("\n\n\nBolt name is -" + boltName + "-full bolt-" + bolt + "-bolt_get Common result-" + bolt.get_common());

                            //parsing code from topology config
                            //                    Firstmap.put("site","uh#slot1,1,1/uh#slot2,1");

                            //                    JSONObject conf = (JSONObject) parser.parse(bolt.get_common().get_json_conf());

                            StateFromConf.createSetFromConf(jsonfilepath, topoName, boltName, vm_Name_supIDMap, boltName_Set_FromConf, workerslot_Set_FromConf, FullMappingRes_conf);//state from conf
                            String boltMappingConfig = JsonFIleReader.getJsonConfig(jsonfilepath, topoName, boltName);
                            String boltMappingThreads = JsonFIleReader.getJsonThreadCount(jsonfilepath, topoName, boltName);
//                        System.out.println("-boltMappingConfig-" + boltMappingConfig + "-boltMappingThreads-" + boltMappingThreads);

                            List<String> mappings = new ArrayList();
                            executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(boltName);
                            System.out.println("executors within this bolt- " + executors);
                            int NoOfexecutors;
                            int index = 0;
                            mappings = Arrays.asList(boltMappingConfig.split("/"));
//                        System.out.println("mappings -" + mappings);
                            Iterator mapping_itr = mappings.iterator();

                            //idea1:using first as key
                            //                    String KeyforMap=mappings.get(0).split(",")[0];
                            //                    vmSlotExecMapping.put(KeyforMap, executors);

                            int curentBoltCount = Integer.parseInt(boltMappingThreads); //conf thread count here
                            int totalExecconf = 0;
                            for (String param : mappings) {
                                totalExecconf += Integer.parseInt(param.split(",")[1]);
                            }
                            System.out.println("checking JSON_total_thread ---> JSON_conf_thread_Mapping......... \n");
                            System.out.println("curentBoltThreadCount-" + curentBoltCount + "-totalExecconf-" + totalExecconf);
                            if (curentBoltCount != totalExecconf) {

                                System.out.println("\n\n\n\t\t**********Parallelism Hint and Conf value are not equal !!! Please see Map in topo for bolt -" + boltName);
                                System.out.println("\t\t*********Please kill topology****");
                                System.out.println("\t\ttopology name----" + t.getName());
                                System.out.println("\t\t*************EXITING**************\n\n\n\n");
                            }

                            //                    if (executors != null && executors.size() != totalExecconf) {
                            //                        System.out.println("**********Parallelism Hint and Conf value are not equal !!! Please see Map in topo for bolt -" + name + "*********EXITING****");
                            //
                            //                    }
                            else {

                                UtilityFunction.putExecListToboltnameMapping(boltName, executors, execToboltNameMapping);//check for null first
                                while (mapping_itr.hasNext() && executors != null) {
                                    String s = (String) mapping_itr.next();
                                    String KeyforMap = s.split(",")[0];
                                    NoOfexecutors = Integer.parseInt(s.split(",")[1]);
//                                System.out.println("KeyforMap -" + KeyforMap + "- index -" + index + "- NoOfexecutors -" + NoOfexecutors);

                                    //logic replacing sublist concept

                                    List<ExecutorDetails> executorDetailses = new ArrayList(executors.subList(index, index + NoOfexecutors));
//                                System.out.println("inside iterator loop - " + KeyforMap + "-----" + executorDetailses);
                                    if (vmSlotExecMapping.containsKey(KeyforMap)) {
//                                    System.out.println("ALready key inserted -" + KeyforMap);
                                        List<ExecutorDetails> prevList = vmSlotExecMapping.get(KeyforMap);
                                        prevList.addAll(executorDetailses);
//                                    System.out.println("after adding -" + prevList);
                                    } else {
                                        vmSlotExecMapping.put(KeyforMap, executorDetailses);
                                    }
                                    index += NoOfexecutors;
                                }
                                System.out.println("vmSlotExecMapping key/value set-" + vmSlotExecMapping);

                            }
                            //                    for (String exe:vmSlotExecMapping.keySet()){
                            //                        System.out.println(vmSlotExecMapping.get(exe));
                            //                    }

                            //                    System.out.println("vmSlotExecMapping valueset-"+vmSlotExecMapping.values());


                            //                    System.out.println("checking null in supervisor or/not-" + ((String) conf.get(SITE)).split(",")[0].split("#")[0]);
                            //
                            //                    if (conf.get(SITE) != null && supervisors.get(((String) conf.get(SITE)).split(",")[0].split("#")[0]) != null) {  //verify the site name on
                            //                        site = ((String) conf.get(SITE)).split(",")[0].split("#")[0];
                            //                        slotID = ((String) conf.get(SITE)).split(",")[0].split("#")[1];
                            //                        threadCount = ((String) conf.get(SITE)).split(",")[2];
                            //                        System.out.println("TEST:inside topology loop site for that bolt is-" + site + "-given config are -" + site + "-" + slotID + "-" + threadCount);
                            //                    }


                            //                    //logging :getting deatils of worker slot
                            //                    for (WorkerSlot w : workerSlots) {
                            //                        System.out.println("worker slots are - nodeID-" + w.getNodeId() + "-PortNumber-" + w.getPort());
                            //                    }
                            //                    for(int i=0;i<workerSlots.size();i++)
                            //                    {
                            //                        System.out.println("workerSlot.get() function " +workerSlots.get(i));
                            //                    }
                            //

                            //may need to open USE:syso
                            System.out.println("\n\ngetting putExecListToboltnamemapping-");
                            UtilityFunction.getExecListToboltnameMapping(execToboltNameMapping);
                        }

                        //code for scheduling bolts
                        for (String s : vmSlotExecMapping.keySet()) {
                            String vm_name = s.split("#")[0];
                            int port_number_from_Conf = Integer.parseInt(s.split("#")[1]);
//                        System.out.println("VM name is -" + vm_name+"port_number_from_Conf is-"+port_number_from_Conf);
                            SupervisorDetails supervisor = supervisors.get(vm_name);
                            List<WorkerSlot> workerSlots = cluster.getAvailableSlots(supervisor);

                            if (vmSlotExecMapping.get(s) != null) {
                                if (!workerSlots.isEmpty()) {
                                    for (WorkerSlot w : workerSlots) {
                                        if (w.getPort() == port_number_from_Conf) {
                                            System.out.println("worker slots are - nodeID-" + w.getNodeId() + "-PortNumber-" + w.getPort() + "-port_number_from_Conf-" + port_number_from_Conf);
                                            cluster.assign(w, t.getId(), vmSlotExecMapping.get(s));
                                        }
                                    }
                                } else {
                                    System.out.println("No Worker slot is empty for this task-" + s);
                                }
                            }

//                        //logging :getting deatils of worker slot
//                        for (WorkerSlot w : workerSlots) {
//                            System.out.println("worker slots are - nodeID-" + w.getNodeId() + "-PortNumber-" + w.getPort());
//                        }
                        }

                        Map<ExecutorDetails, WorkerSlot> execToslotMapping = new HashMap<ExecutorDetails, WorkerSlot>();
                        execToslotMapping = UtilityFunction.getCurrentExectoSlotMapping(cluster, topoID);//includes spout also


                        List<ExecutorDetails> spout_executors = new ArrayList<>();
                        System.out.println("execToslotMapping-" + execToslotMapping);
                        for (String spoutName : spouts.keySet()) {
                            String site1 = null;
                            SpoutSpec spout = spouts.get(spoutName);
//                        System.out.println("---Reading JSON file---");
                            String spoutMappingConfig = JsonFIleReader.getJsonConfig(jsonfilepath, topoName, spoutName);
                            String boltMappingThreads = JsonFIleReader.getJsonThreadCount(jsonfilepath, topoName, spoutName);
                            //CHECK:No split to this config
                            if (spoutMappingConfig != null && supervisors.get(spoutMappingConfig) != null) {
                                site1 = spoutMappingConfig;
                                System.out.println("TEST:inside topology loop for spout-" + site1);
                            }
                            SupervisorDetails supervisor = supervisors.get(site1);
                            List<WorkerSlot> workerSlots = cluster.getAvailableSlots(supervisor);
                            spout_executors = cluster.getNeedsSchedulingComponentToExecutors(t).get(spoutName);
                            if (!workerSlots.isEmpty() && spout_executors != null) {
                                System.out.println("Going to assign spout" + workerSlots.get(0) + "-" + spout_executors);
                                cluster.assign(workerSlots.get(0), t.getId(), spout_executors);
                                UtilityFunction.putExecListToboltnameMapping(spoutName, spout_executors, execToboltNameMapping);//check for null first
//                            execToslotMapping=UtilityFunction.removeSpout_CurrentExectoSlotMapping(spout_executors,execToslotMapping);
                            }
                        }
                        //printing current assignment
//                    execToslotMapping=UtilityFunction.removeSpout_CurrentExectoSlotMapping(spout_executors,execToslotMapping);//removing spout_executors from  mapping
                        System.out.println("Before calling Join Utility function arg passed -" + execToslotMapping);
                        UtilityFunction.joinExecToboltNameAndgetCurrentExectoSlotmapping(execToslotMapping, execToboltNameMapping, cluster);

                        System.out.println("\n\n\t\t**********CONF state**********");
                        System.out.println("Conf State set-" + boltName_Set_FromConf);
                        System.out.println("Conf State set-" + workerslot_Set_FromConf);
                        StateFromConf.createStateFromConf(boltName_Set_FromConf, workerslot_Set_FromConf, FullMappingRes_conf);
                    }

                } else {
                    System.out.println("\n\n\t\t\t\t--None topo needs scheduling--");
                }
            }
        }

    }


    //        TEST:running
    //        TEST:caching supervisors indexed by their sites in a hash map...
    //        TEST:supervisor name-backtype.storm.scheduler.SupervisorDetails@375112e4
    //        TEST: checking if metadata is set on this supervisor....
    //        TEST:Value for this supervisor-ufl
    //        TEST:supervisor name-backtype.storm.scheduler.SupervisorDetails@2baf531b
    //        TEST: checking if metadata is set on this supervisor....
    //        TEST:Value for this supervisor-tamu
    //        TEST:supervisor name-backtype.storm.scheduler.SupervisorDetails@3792805
    //        TEST: checking if metadata is set on this supervisor....
    //        TEST:Value for this supervisor-uh
    //        ExecutorToComponents  mapping-
    //        Component name--Second
    //        start task-3
    //        start task-3
    //        Component name--__acker
    //        start task-5
    //        start task-5
    //        Component name--Third
    //        start task-4
    //        start task-4
    //        Component name--First
    //        start task-2
    //        start task-2
    //        Component name--AuthSpout
    //        start task-1
    //        start task-1
    //        TEST:inside topology loop for bolt-ufl
    //        TEST:inside topology loop for bolt-tamu
    //        TEST:inside topology loop for bolt-uh
    //        TEST:inside topology loop for spout-tamu