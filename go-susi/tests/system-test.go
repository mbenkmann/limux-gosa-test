package tests

import (
         "io"
         "io/ioutil"
         "net"
         "fmt"
         "path"
         "sort"
         "time"
         "runtime"
         "syscall"
         "strings"
         "strconv"
         "os"
         "os/exec"
         "encoding/base64"
         
         "../db"
         "../xml"
         "github.com/mbenkmann/golib/util"
         "../config"
         "../security"
       )


// true if we're testing gosa-si instead of go-susi
var gosasi bool

// true if the daemon we're testing was launched by us (rather than being prelaunched)
var launched_daemon bool

// start time of SystemTest()
var StartTime time.Time

// the temporary directory where log and db files are stored
var confdir string

// if launched_daemon == true, this gives access to the process of the daemon.
var daemonProcess *os.Process

// Runs the system test.
//  daemon: either "", host:port or the path to a binary. 
//         If "", the default from the config will be used.
//         If host:port, the daemon running at that address will be tested. 
//         Some tests cannot be run in this case.
//         If a program path is used, the program will be launched with
//         -f -c tempfile  where tempfile is a generated config file that
//         specifies the SystemTest's listener as a peer server, so that
//         e.g. new_server messages can be tested.
//  is_gosasi: if true, test evaluation will be done for gosa-si. This does not
//         affect the tests being done, only whether fails/passes are counted as
//         expected or unexpected.
func SystemTest(daemon string, is_gosasi bool) {
  fmt.Println(`
#############################################################################
####################### S Y S T E M   T E S T ###############################
#############################################################################
`)
  
  gosasi = is_gosasi
  launched_daemon = !strings.Contains(daemon,":")
  if gosasi { reply_timeout *= 10 }
  
  StartTime = time.Now()
  
  // start our own "server" that will take messages
  listen()
  time.Sleep(1*time.Second) // give the server time to start up
  
  config.ReadNetwork()
  listen_address = config.IP + ":" + listen_port
  client_listen_address = config.IP + ":" + client_listen_port
  
  // if we got a program path (i.e. not host:port), create config and launch slapd
  if launched_daemon {
    //first launch the test ldap server
    cmd := exec.Command("/usr/sbin/slapd","-f","./slapd.conf","-h","ldap://127.0.0.1:20088","-d","0")
    cmd.Dir = "./testdata"
    err := cmd.Start()
    if err != nil { panic(err) }
    time.Sleep(2*time.Second) // give LDAP server time to start
    defer cmd.Process.Signal(syscall.SIGTERM)
    config.ServerConfigPath, confdir = createConfigFile("system-test-", listen_address)
    //defer os.RemoveAll(confdir)
    defer fmt.Printf("\nLog file directory: %v\n", confdir)
  }
  
  // this reads either the default config or the one we created above
  config.Init()
  config.ReadConfig()
  defer config.Shutdown()
  
  config.Timeout = reply_timeout
  
  if !launched_daemon {
    config.ServerSourceAddress = daemon
  }
  
  config.ClientDBPath = confdir + "/dummy-clientdb.xml"
  db.ClientsInit()
  
  // At this point:
  //   listen_address is the address of the test server run by listen()
  //   config.ServerSourceAddress is the address of the go-susi or gosa-si being tested  
  
  init_keys()
  
  // now that our server is ready, start the daemon to be tested if necessary
  if launched_daemon {
    args := []string{ "-f", "-vvvvvv"}
    if !gosasi { args = append(args,"--test="+confdir) }
    args = append(args, "-c", config.ServerConfigPath)
    cmd := exec.Command(daemon, args...)
    cmd.Stderr,_ = os.Create(confdir+"/go-susi+panic.log")
    err := cmd.Start()
    if err != nil { panic(err) }
    time.Sleep(5*time.Second) // give daemon a little time to start up
    daemonProcess = cmd.Process
    defer cmd.Process.Signal(syscall.SIGTERM)
    
    // Give daemon time to process data and write logs before sending SIGTERM
    defer time.Sleep(reply_timeout)
  }
  
  run_startup_tests()
  check_connection_drop_on_error1()
  check_connection_drop_on_error2()
  
  check_multiple_requests_over_one_connection()
  
  // query for trigger_action_lock on test_mac2
  x := gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v)))))", Jobs[1].MAC))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  check(x.Text("header"), "query_jobdb")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  a := x.First("answer1")
  check(a != nil, true)
  if a != nil {
    check_answer(a, Jobs[1].Plainname, "none", "waiting", config.ServerSourceAddress, Jobs[1].MAC, Jobs[1].Timestamp, Jobs[1].Periodic, Jobs[1].Trigger())
  }
  
  // query for trigger_action_wake on test_mac (via "ne Jobs[1].MAC")
  x = gosa("query_jobdb", hash("xml(where(clause(connector(and)phrase(operator(ne)macaddress(%v)))))", Jobs[1].MAC))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  check(x.Text("header"), "query_jobdb")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  a = x.First("answer1")
  check(a != nil, true)
  if a != nil {
    check_answer(a, Jobs[0].Plainname, "none", "waiting", config.ServerSourceAddress, Jobs[0].MAC, Jobs[0].Timestamp, Jobs[0].Periodic, Jobs[0].Trigger())
  }
  
  // delete trigger_action_wake on test_mac (via "ne test_mac2" plus redundant "like ...")
  t0 := time.Now()
  x = gosa("delete_jobdb_entry", hash("xml(where(clause(connector(and)phrase(operator(like)headertag(trigger_action_%%))phrase(operator(ne)macaddress(%v)))))", Jobs[1].MAC))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  check(x.Text("header"), "answer")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  check(x.Text("answer1"), "0")
  
  // query complete jobdb. The reply should only be one remaining job.
  // Depending on timing this may fail on gosa-si because it allows jobs to
  // be observed in "done" status. However on go-susi this should always
  // give the expected result.
  x = gosa("query_jobdb", hash("xml(where())"))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  check(x.Text("header"), "query_jobdb")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  a = x.First("answer1")
  if a == nil { a = xml.NewHash("answer1") }
  old_job := a.Clone()
  check(a != nil, true)
  if a != nil {
    check_answer(a, Jobs[1].Plainname, "none", "waiting", config.ServerSourceAddress, Jobs[1].MAC, Jobs[1].Timestamp, Jobs[1].Periodic, Jobs[1].Trigger())
  }
  
  // check for foreign_job_updates with status "done"
  msg := wait(t0, "foreign_job_updates")
  check_foreign_job_updates(msg, keys[0], config.ServerSourceAddress, Jobs[0].Plainname, "", "done", "none", Jobs[0].MAC, Jobs[0].Trigger(), Jobs[0].Timestamp)
  
  // Send foreign_job_updates with following changes:
  //   change <progress> of the existing job
  //   add a new job
  old_job.FirstOrAdd("progress").SetText("42")
  new_job := hash("answer2(plainname(%v)progress(none)status(waiting)siserver(localhost)modified(1)macaddress(%v)targettag(%v)timestamp(%v)id(66)headertag(%v)result(none))",Jobs[2].Plainname,Jobs[2].MAC,Jobs[2].MAC,Jobs[2].Timestamp,Jobs[2].Trigger())
  new_job.FirstOrAdd("xmlmessage").SetText(base64.StdEncoding.EncodeToString([]byte(hash("xml(header(%v)source(GOSA)target(%v)timestamp(%v)macaddress(%v))",Jobs[2].Type,Jobs[2].MAC,Jobs[2].Timestamp,Jobs[2].MAC).String())))
  x = hash("xml(header(foreign_job_updates)source(%v)target(%v))",listen_address,config.ServerSourceAddress)
  x.AddClone(old_job)
  x.AddClone(new_job)
  send("", x)
  
  // Wait for message to be processed, because send() doesn't wait.
  time.Sleep(reply_timeout)
  
  // Check the jobdb for the above changes
  x = gosa("query_jobdb", hash("xml(where())"))
  check(checkTags(x, "header,source,target,answer1,answer2,session_id?"),"")
  check(x.Text("header"), "query_jobdb")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  a1 := x.First("answer1")
  a2 := x.First("answer2")
  if a1 != nil && a2 != nil{
    if a1.Text("plainname") == Jobs[2].Plainname { // make sure a1 is the old and a2 is new job
      a1, a2 = a2, a1
    }
    
    check_answer(a1, Jobs[1].Plainname, "42", "waiting", config.ServerSourceAddress, Jobs[1].MAC, Jobs[1].Timestamp, Jobs[1].Periodic, Jobs[1].Trigger())
    
    check_answer(a2, Jobs[2].Plainname, "none", "waiting", listen_address, Jobs[2].MAC, Jobs[2].Timestamp, Jobs[2].Periodic, Jobs[2].Trigger())
  }

  // Shut down our test server and active connections
  listen_stop()
  
  // Wait a little so that the testee notices
  time.Sleep(1*time.Second)
  // Now test that our test server's jobs are marked as state "error" in query_jobdb
  x = gosa("query_jobdb", hash("xml(where(clause(phrase(siserver(%v)))))",listen_address))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  a2 = x.First("answer1")
  check_answer(a2, Jobs[2].Plainname, "none", "error", listen_address, Jobs[2].MAC, Jobs[2].Timestamp, Jobs[2].Periodic, Jobs[2].Trigger())
  
  // Restart our test server
  t0 = time.Now()
  listen()
  
  // Wait for the peer to re-establish the connection
  for i:=0; i<30 && active_connections.IsEmpty(); i++ {
    time.Sleep(1*time.Second)
  }
  
  // Check for the new_server message we should get after the connection is re-established
  time.Sleep(2*time.Second)
  for _,msg = range get(t0) {
    if msg.XML.Text("header") == "new_server" { break }
  }
  
  // Send back confirm_new_server
  send("[ServerPackages]", hash("xml(header(confirm_new_server)confirm_new_server()key(%v)loaded_modules(goSusi)macaddress(00:00:00:00:00:00))", msg.XML.Text("key")))
  
  // Check for the <sync>all</sync> message we should get after the connection
  // is re-established
  time.Sleep(2*time.Second)
  for _,msg = range get(t0) {
    if msg.XML.Text("sync") == "all" { break }
  }
  check(msg.XML.Text("sync"),"all")
  check_foreign_job_updates(msg, keys[0], config.ServerSourceAddress, Jobs[1].Plainname, Jobs[1].Periodic, "waiting", "42", Jobs[1].MAC, Jobs[1].Trigger(), Jobs[1].Timestamp)
  
  // Now test that our test server's jobs are no longer in error state in query_jobdb
  x = gosa("query_jobdb", hash("xml(where(clause(phrase(siserver(%v)))))",listen_address))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  a2 = x.First("answer1")
  check_answer(a2, Jobs[2].Plainname, "none", "waiting", listen_address, Jobs[2].MAC, Jobs[2].Timestamp, Jobs[2].Periodic, Jobs[2].Trigger())

  // clear jobdb  
  gosa_noreply("delete_jobdb_entry", hash("xml(where())"))
  
  // Because the above delete affects a job belonging to the test server,
  // go-susi doesn't delete it directly but forwards the request to the
  // test server. Wait a little to make sure the communication is finished.
  time.Sleep(config.GosaQueryJobdbMaxDelay)
  
  // now add 2 jobs that are the same in all respects except the timestamp
  x = gosa("job_trigger_action_lock", hash("xml(target(%v)timestamp(%v)macaddress(%v)periodic(%v))",Jobs[1].MAC, Jobs[0].Timestamp, Jobs[1].MAC, Jobs[1].Periodic))
  check(x.Text("answer1"), "0")
  x = gosa("job_trigger_action_lock", hash("xml(target(%v)timestamp(%v)macaddress(%v)periodic(%v))",Jobs[1].MAC, Jobs[1].Timestamp, Jobs[1].MAC, Jobs[1].Periodic))
  check(x.Text("answer1"), "0")

  // Because the plainname is filled in by a background job, wait a little
  // or the plainname subtest further below may fail.
  time.Sleep(config.GosaQueryJobdbMaxDelay)

  x = gosa("query_jobdb", hash("xml(where())"))
  siFail(checkTags(x, "header,source,target,answer1,answer2,session_id?"),"")
  a1 = x.First("answer1")
  a2 = x.First("answer2")
  if a1 != nil && a2 != nil{
    if a1.Text("timestamp") == Jobs[1].Timestamp { // make sure a1 has Jobs[0].Timestamp
      temp := a1
      a1 = a2
      a2 = temp
    }
    
    check_answer(a1, Jobs[1].Plainname, "none", "waiting", config.ServerSourceAddress, Jobs[1].MAC, Jobs[0].Timestamp, Jobs[1].Periodic, Jobs[1].Trigger())
    check_answer(a2, Jobs[1].Plainname, "none", "waiting", config.ServerSourceAddress, Jobs[1].MAC, Jobs[1].Timestamp, Jobs[1].Periodic, Jobs[1].Trigger())
  }

  // Test if server understands messages with ";IP:PORT" attached (gosa-si 2.7 protocol)
  conn, err := net.Dial("tcp", config.ServerSourceAddress)
  check(err, nil)
  encrypted_msg := security.GosaEncrypt("<xml><header>gosa_query_jobdb</header><where></where><source>GOSA</source><target>GOSA</target></xml>", config.ModuleKey["[GOsaPackages]"])
  util.SendLn(conn, encrypted_msg + ";"+listen_address , config.Timeout)
  reply, err := util.ReadLn(conn, config.Timeout)
  if err == nil {
    reply = security.GosaDecrypt(reply, config.ModuleKey["[GOsaPackages]"])
    x, err = xml.StringToHash(reply)
  }
  if err != nil { x = xml.NewHash("error") }
  if conn != nil { conn.Close() }
  check(checkTags(x, "header,source,target,answer1,answer2,session_id?"),"")
  
  // Test if the server rejects messages encrypted with dummy-key or unencrypted
  for repcount := 0; repcount < 2; repcount++ {
    conn, err := net.Dial("tcp", config.ServerSourceAddress)
    check(err, nil)
    encrypted_msg := "<xml><header>gosa_query_jobdb</header><where></where><source>GOSA</source><target>GOSA</target></xml>"
    if repcount == 1 { encrypted_msg = security.GosaEncrypt(encrypted_msg, "dummy-key") }
    util.SendLn(conn, encrypted_msg, config.Timeout)
    reply, err := util.ReadLn(conn, config.Timeout)
    reply = security.GosaDecrypt(reply, "dummy-key")
    check(strings.Contains(reply,"<error_string>"), true)
    if conn != nil { conn.Close() }
  }
  
  if gosasi {
    // The job processing tests require the following go-susi extensions:
    // * multiple jobs with the same headertag+macaddress combination
    // * <periodic> with unit "seconds"
    // Therefore we skip them if we're testing gosa-si.
    fmt.Print("Job processing tests need go-susi extensions => ")
    siFail(true,false)
  } else {
    run_job_processing_tests()
  }

  run_foreign_job_updates_tests()
  
  run_here_i_am_tests()
  
  if gosasi {
    // The foreign client tests check success by examining clientdb.xml directly.
    // There is not code to do the same with gosa-si's db files.
    // Therefore we skip these tests if we're testing gosa-si.
    fmt.Print("Foreign client tests not implemented for gosa-si => ")
    siFail(true,false)
  } else {
    run_foreign_client_tests()
  }
  
  run_object_group_inheritance_tests()
  
  run_detected_hardware_tests()

  run_fai_query_tests()
  
  if launched_daemon {
    if gosasi {
      fmt.Print("gosa-si does not support hooks => ")
      siFail(true,false)
    } else {
      run_hook_tests()
    }
  }

  if gosasi {
    fmt.Print("gosa-si does not support re-directing log files to --temp dir => ")
    siFail(true,false)
    fmt.Print("gosa-si handles job_trigger_activate_new different from go-susi => ")
    siFail(true,false)
    fmt.Print("gosa-si does not include a TFTP server => ")
    siFail(true,false)
    fmt.Print("gosa-si does not support new_foo_config messages => ")
    siFail(true,false)
  } else {
    run_save_fai_log_tests()
    run_postprocess_logfiles_tests()
    run_trigger_activate_new_tests()
    run_tftp_tests()
    run_new_foo_config_tests()
    run_audit_tests()
  }
  
  run_activate_new_client_test()
  run_gosa_ping_tests()
}

func run_audit_tests() {
  gosa("job_trigger_action_audit", hash("xml(macaddress(%v)target(%v))",config.MAC,config.ServerSourceAddress))
  time.Sleep(2*time.Second)
  x := gosa("query_audit", hash("xml(audit(bar)select(sirene)select(sirene)where(clause(phrase(key(%v)))))", "feuerwehr"))
  check(checkTags(x, "header,source,target,answer1,known,unknown,session_id?"),"")
  check(x.Text("header"), "query_audit")
  check(x.Text("known"), "1")
  check(x.Text("unknown"), "1")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  a := x.First("answer1")
  if check(a != nil, true) {
    check(checkTags(a,"sirene"), "")
    check(a.Text("sirene"),"laut")
  }
  
  x = gosa("query_audit", hash("xml(includeothers()audit(bar)select(macaddress)select(key)where(clause(phrase(sirene(%v)))))", "nervig"))
  check(checkTags(x, "header,source,target,noaudit,answer1,known,unknown,session_id?"),"")
  check(x.Text("header"), "query_audit")
  check(x.Text("known"), "1")
  check(x.Text("unknown"), "1")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  a = x.First("answer1")
  if check(a != nil, true) {
    check(checkTags(a,"key,macaddress"), "")
    check(a.Text("key"),"bullizei")
    check(a.Text("macaddress"),config.MAC)
  }
  a = x.First("noaudit")
  if check(a != nil, true) {
    check(checkTags(a,"macaddress"), "")
    check(a.Text("macaddress"),strings.ToLower(Jobs[1].MAC))
  }
  
  x = gosa("query_audit", hash("xml(includeothers()audit(foo)select(macaddress)select(key)where(clause(phrase(sirene(%v)))))", "nervig"))
  check(checkTags(x, "header,source,target,noaudit+,known,unknown,session_id?"),"")
  check(x.Text("header"), "query_audit")
  check(x.Text("known"), "0")
  check(x.Text("unknown"), "2")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  macs := []string{}
  a = x.First("noaudit")
  if check(a != nil, true) {
    check(checkTags(a,"macaddress"), "")
    macs = append(macs, a.Text("macaddress"))
  }
  a = a.Next()
  if check(a != nil, true) {
    check(checkTags(a,"macaddress"), "")
    macs = append(macs, a.Text("macaddress"))
  }
  
  if check(len(macs),2) {
    if macs[1] == config.MAC {
      check(macs[0],strings.ToLower(Jobs[1].MAC))
      check(macs[1],config.MAC)
    } else {
      check(macs[1],strings.ToLower(Jobs[1].MAC))
      check(macs[0],config.MAC)
    }
  }
  
  x = gosa("query_audit", hash("xml(includeothers()audit(bar)select(macaddress)select(key))"))
  check(checkTags(x, "header,answer1,answer2,noaudit,source,target,known,unknown,session_id?"),"")
  check(x.Text("header"), "query_audit")
  check(x.Text("known"), "1")
  check(x.Text("unknown"), "1")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  a = x.First("answer1")
  if check(a != nil, true) {
    check(checkTags(a,"macaddress,key"), "")
    check(a.Text("macaddress"),config.MAC)
    check(a.Text("key"),"feuerwehr")
  }
  a = x.First("answer2")
  if check(a != nil, true) {
    check(checkTags(a,"macaddress,key"), "")
    check(a.Text("macaddress"),config.MAC)
    check(a.Text("key"),"bullizei")
  }
  a = x.First("noaudit")
  if check(a != nil, true) {
    check(checkTags(a,"macaddress"), "")
    check(a.Text("macaddress"),strings.ToLower(Jobs[1].MAC))
  }
  
  x = gosa("query_audit_aggregate", hash("xml(includeothers()audit(bar)select(macaddress)count(as(key)unique(key)))"))
  check(checkTags(x, "header,answer1,noaudit,aggregate,source,target,known,unknown,session_id?"),"")
  check(x.Text("header"), "query_audit_aggregate")
  check(x.Text("known"), "1")
  check(x.Text("unknown"), "1")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  a = x.First("answer1")
  if check(a != nil, true) {
    check(checkTags(a,"macaddress,key"), "")
    check(a.Text("macaddress"), config.MAC)
    check(a.Text("key"),"2")
  }
  a = x.First("aggregate")
  if check(a != nil, true) {
    check(checkTags(a,"key"), "")
    check(a.Text("key"),"2")
  }
}

func run_gosa_ping_tests() {
  mac := "aa:00:bb:11:cc:99"
  hia := hash("xml(header(here_i_am)source(%v)target(%v)new_passwd(%v)mac_address(%v))", client_listen_address, config.ServerSourceAddress, keys[len(keys)-1], mac)
  send("[ClientPackages]", hia)
  time.Sleep(reply_timeout)
  
  var reply string
  conn, err := net.Dial("tcp", config.ServerSourceAddress)
  if err != nil {
    reply = err.Error()
  } else {
    util.SendLn(conn, security.GosaEncrypt("<xml><header>gosa_ping</header><source>GOSA</source><target>"+mac+"</target></xml>", config.ModuleKey["[GOsaPackages]"]), config.Timeout)
    reply, _ = util.ReadLn(conn, config.Timeout)
    reply = security.GosaDecrypt(reply, config.ModuleKey["[GOsaPackages]"])
    if reply != "" { reply = "" } else { reply = "error" }
    conn.Close()
  }
  
  check(reply, "")
  
  conn, err = net.Dial("tcp", config.ServerSourceAddress)
  if err != nil {
    reply = err.Error()
  } else {
    util.SendLn(conn, security.GosaEncrypt("<xml><header>gosa_ping</header><source>GOSA</source><target>0f:C3:d2:Aa:11:22</target></xml>", config.ModuleKey["[GOsaPackages]"]), config.Timeout)
    reply,_ = util.ReadLn(conn, config.Timeout)
    reply = security.GosaDecrypt(reply, config.ModuleKey["[GOsaPackages]"])
    conn.Close()
  }
  
  check(reply, "")
}

func run_activate_new_client_test() {
  // send here_i_am for non-existing LDAP object
  // check that we get registered
  // check that we get detect_hardware
  // check that job exists with status "processing", progress "hardware-detection"
  // send detected_hardware
  // check that LDAP object has been created
  // send CLMSG_GOTOACTIVATION
  // check that job exists with status "processing", progress "goto-activation"
  // send gosa_set_activated_for_installation
  // check that we received set_activated_for_installation
  // check faistate "install" and gotoMode "active"
  // check that job exists with status "processing"
  // send progress 50
  // check that job exists with status "processing" and progress "50"
  // send log files
  // check that faistate is "localboot"
  // check that job is gone
  
  gosa("delete_jobdb_entry", hash("xml(where())"))
  time.Sleep(reply_timeout)

  mac := "aa:00:bb:11:cc:99"

  // make sure there's no old entry from a (crashed) previous run
  sys,_ := db.SystemGetAllDataForMAC(mac, false)
  db.SystemReplace(sys, nil)
  
  hia := hash("xml(header(here_i_am)source(%v)target(%v)new_passwd(%v)mac_address(%v))", client_listen_address, config.ServerSourceAddress, keys[len(keys)-1], mac)
  t0 := time.Now()
  send("[ClientPackages]", hia)
  check(waitlong(t0, "detect_hardware").XML.Text("header"),"detect_hardware")
  check(waitlong(t0, "registered").XML.Text("header"),"registered")
  
  job := gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v)))clause(phrase(headertag(trigger_action_reinstall)))))",mac))
  if check(checkTags(job, "header,source,target,session_id?,answer1"),"") {
    check(job.First("answer1").Text("status"), "processing")
    check(job.First("answer1").Text("progress"), "hardware-detection")
  }
  
  detected_hardware := hash("xml(header(detected_hardware)source(%v))",client_listen_address)
  dh := hash("detected_hardware(gotoModules(m1)goTOModules(m2)gotoxdriver(notemplate))")
  detected_hardware.AddWithOwnership(dh)
  dh2 := "<detected_hardware ghMemSize='12345' gotoLdapServer='1:ldap01.tvc.example.com:ldap://ldap01.tvc.example.com/o=go-susi,c=de' gotoSndModule=\"snd_noisemaster\"><gotoMODULES>m3</gotoMODULES><GOTOModulES>m4</GOTOModulES></detected_hardware>"
  dh_string := strings.Replace(detected_hardware.String(),"<detected_hardware>",dh2+"<detected_hardware>",-1)
  t0 = time.Now()
  util.SendLnTo(config.ServerSourceAddress, security.GosaEncrypt(dh_string, keys[len(keys)-1]), config.Timeout)
  check(waitlong(t0, "set_activated_for_installation").XML,"<xml></xml>") //check that we do NOT received safi
  check(waitlong(t0, "new_ldap_config").XML.Text("ldap_uri"), "ldap://ldap01.tvc.example.com")
  sys,err := db.SystemGetAllDataForMAC(mac, false)
  if check(err,nil) {
    check(sys.Text("faistate"), "install")
    
    job = gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v)))clause(phrase(headertag(trigger_action_reinstall)))))",mac))
    if check(checkTags(job, "header,source,target,session_id?,answer1"),"") {
      check(job.First("answer1").Text("status"), "processing")
      check(job.First("answer1").Text("progress"), "hardware-detection")
    }
    
    gotoactivation := hash("xml(header(CLMSG_GOTOACTIVATION)source(%v)target(%v)macaddress(%v))", client_listen_address, config.ServerSourceAddress, mac)
    send("CLIENT", gotoactivation)
    
    time.Sleep(reply_timeout)
    
    job = gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v)))clause(phrase(headertag(trigger_action_reinstall)))))",mac))
    if check(checkTags(job, "header,source,target,session_id?,answer1"),"") {
      check(job.First("answer1").Text("status"), "processing")
      check(job.First("answer1").Text("progress"), "goto-activation")
    }
    
    t0 = time.Now()
    gosa_noreply("set_activated_for_installation", hash("xml(target(%v)macaddress(%v))",mac,mac))
    safi := waitlong(t0, "set_activated_for_installation").XML
    if check(checkTags(safi, "header,source,target,faistate,set_activated_for_installation"),"") {
      check(safi.Text("header"), "set_activated_for_installation")
      check(safi.Text("faistate"), "install")
    }
    check(db.SystemGetState(mac, "faistate"), "install")
    check(db.SystemGetState(mac, "gotomode"), "active")
    
    t0 = time.Now()
    gosa("trigger_action_activate", hash("xml(target(%v)macaddress(%v))",mac,mac))
    safi = waitlong(t0, "set_activated_for_installation").XML
    if check(checkTags(safi, "header,source,target,faistate,set_activated_for_installation"),"") {
      check(safi.Text("header"), "set_activated_for_installation")
      check(safi.Text("faistate"), "install")
    }
    
    job = gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v)))clause(phrase(headertag(trigger_action_reinstall)))))",mac))
    if check(checkTags(job, "header,source,target,session_id?,answer1"),"") {
      check(job.First("answer1").Text("status"), "processing")
    }
    
    progress := hash("xml(header(CLMSG_PROGRESS)source(%v)target(%v)macaddress(%v)CLMSG_PROGRESS(50))", client_listen_address, config.ServerSourceAddress, mac)
    send("CLIENT", progress)
    
    time.Sleep(reply_timeout)
    
    job = gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v)))clause(phrase(headertag(trigger_action_reinstall)))))",mac))
    if check(checkTags(job, "header,source,target,session_id?,answer1"),"") {
      check(job.First("answer1").Text("status"), "processing")
      check(job.First("answer1").Text("progress"), "50")
    }
    
    logs := hash("xml(header(CLMSG_save_fai_log)CLMSG_save_fai_log()source(%v)target(%v)macaddress(%v)fai_action(install))",client_listen_address, config.ServerSourceAddress, mac)
    send("CLIENT", logs)
    time.Sleep(reply_timeout)
    
    job = gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v)))clause(phrase(headertag(trigger_action_reinstall)))))",mac))
    check(job.First("answer1"), nil)
    
    check(db.SystemGetState(mac, "faistate"), "localboot")
  }
  
  db.SystemReplace(sys, nil)
}

func run_new_foo_config_tests() {
  os.Remove(path.Join(confdir,"config.txt"))
  newcfg1 := hash("xml(header(new_ldap_config)admin_base(A)department(D)ldap_uri(ldap://foo)ldap_uri(ldap://bar))")
  newcfg2 := hash("xml(header(new_ntp_config)server(foo)server(bar))")
  send("[GOsaPackages]", newcfg1)
  time.Sleep(1*time.Second)
  send("[GOsaPackages]", newcfg2)
  time.Sleep(1*time.Second)
  data, err := ioutil.ReadFile(path.Join(confdir,"config.txt"))
  if check(err, nil) {
    check(string(data), "D\nA\nldap://foo\nldap://bar\nfoo\nbar\n")
  }
}

func run_tftp_tests() {
  tftp_addr,err := net.ResolveUDPAddr("udp", "localhost:"+config.TFTPPort)
  if err != nil { panic(err) }
  local_addr,err := net.ResolveUDPAddr("udp", ":45672")
  if err != nil { panic(err) }
  conn, err := net.ListenUDP("udp", local_addr)
  if err != nil { panic(err) }
  //conn,err := net.DialUDP("udp", local_addr, addr)
  if check(err, nil) {
    defer conn.Close()
    if err != nil { panic(err) }
    n, err := conn.WriteToUDP([]byte("\000\001pxelinux.0\000octet\000tsize\0000\000blksize\0007\000"), tftp_addr)
    check(err,nil)
    buf := make([]byte,256)
    conn.SetReadDeadline(time.Now().Add(200*time.Millisecond))
    n, remote_addr, err := conn.ReadFromUDP(buf)
    check(err,nil)
    if check(n > 8, true) {
      check(buf[0:2],[]byte{0,6})
      parts := strings.Split(string(buf[2:n]),"\000")
      check(len(parts),5)
      if parts[0] == "blksize" {
        parts[0],parts[1],parts[2],parts[3] = parts[2],parts[3],parts[0],parts[1]
      }
      check(parts[2],"blksize")
      check(parts[3],"7")
      check(parts[4],"")
      check(parts[0],"tsize")
      datasize,err := strconv.Atoi(parts[1])
      check(err, nil)
      
      // We pretend that we sent an ACK that got lost. The TFTP server should
      // re-transmit its OACK
      
      conn.SetReadDeadline(time.Now().Add(2*time.Second))
      n, remote_addr2, err := conn.ReadFromUDP(buf)
      check(err,nil)
      if check(n > 8, true) {
        check(remote_addr2, remote_addr)
        check(buf[0:2],[]byte{0,6})
        parts = strings.Split(string(buf[2:n]),"\000")
        check(len(parts),5)
        if parts[0] == "blksize" {
          parts[0],parts[1],parts[2],parts[3] = parts[2],parts[3],parts[0],parts[1]
        }
        check(parts[2],"blksize")
        check(parts[3],"7")
        check(parts[4],"")
        check(parts[0],"tsize")
        datasize,err = strconv.Atoi(parts[1])
        check(err, nil)
      }
      
      _, err = conn.WriteToUDP([]byte{0,4,0,0}, remote_addr)
      check(err,nil)
      
      cmp,_ := ioutil.ReadFile(path.Join(confdir,"pxelinux.txt")) //.txt not .0!
      check(datasize,len(cmp))
      
      start := 0
      blocksize := 7
      blockid := 1
      test_dup := true
      for {
        sz := len(cmp) - start
        if sz > blocksize { sz = blocksize }
        conn.SetReadDeadline(time.Now().Add(1*time.Second))
        n, remote_addr, err := conn.ReadFromUDP(buf)
        check(err,nil)
        if !check(n > 4, true) { break }
        check(buf[0], 0)
        check(buf[1], 3)
        check(buf[2], 0)
        check(buf[3], blockid&0xff)
        check(string(buf[4:n]), string(cmp[start:start+sz]))
        
        // We pretend that we sent an ACK that got lost. The TFTP server should
        // re-transmit its DATA
        conn.SetReadDeadline(time.Now().Add(2*time.Second))
        n, remote_addr2, err = conn.ReadFromUDP(buf)
        check(err,nil)
        if check(n > 4, true) {
          check(remote_addr2, remote_addr)
          check(buf[0], 0)
          check(buf[1], 3)
          check(buf[2], 0)
          check(buf[3], blockid&0xff)
          check(string(buf[4:n]), string(cmp[start:start+sz]))
        }
        
        // every 2nd block we pretend that we didn't get DATA and resend
        // ACK with the previous block id.
        test_dup = !test_dup
        if test_dup {
          _, err = conn.WriteToUDP([]byte{0,4,0,byte(blockid-1)}, remote_addr)
          check(err,nil)
          continue
        }
        
        _, err = conn.WriteToUDP([]byte{0,4,0,byte(blockid)}, remote_addr)
        check(err,nil)
        start += sz
        blockid++    
        if sz < blocksize { break }
      }
    }
    
    _, err = conn.WriteToUDP([]byte("\000\001doesntexist\000octet\000"), tftp_addr)
    check(err,nil)
    conn.SetReadDeadline(time.Now().Add(3*time.Second))
    n, _, err = conn.ReadFromUDP(buf)
    check(err,nil)
    if check(n >= 4, true) {
      check(buf[0:4], []byte{0,5,0,1})
    }
    
    _, err = conn.WriteToUDP([]byte("\000\001false\000octet\000"), tftp_addr)
    check(err,nil)
    conn.SetReadDeadline(time.Now().Add(3*time.Second))
    n, _, err = conn.ReadFromUDP(buf)
    check(err,nil)
    if check(n >= 4, true) {
      check(buf[0:4], []byte{0,5,0,1})
    }
    
    _, err = conn.WriteToUDP([]byte("\000\001blarg\000octet\000"), tftp_addr)
    check(err,nil)
    conn.SetReadDeadline(time.Now().Add(3*time.Second))
    n, _, err = conn.ReadFromUDP(buf)
    check(err,nil)
    if check(n >= 4, true) {
      check(buf[0:4], []byte{0,5,0,1})
    }
    
    _,err = conn.WriteToUDP([]byte("\000\001pxelinux.cfg/01-0a-0b-0c-0d-0e-0f\000octet\000"),tftp_addr)
    check(err,nil)
    conn.SetReadDeadline(time.Now().Add(3*time.Second))
    n, remote_addr, err = conn.ReadFromUDP(buf)
    check(err,nil)
    if check(n >= 4, true) {
      check(buf[0:4], []byte{0,3,0,1})
      check(string(buf[4:n]), "0a:0b:0c:0d:0e:0f\npxelinux.cfg/01-0a-0b-0c-0d-0e-0f\n")
      // send ACK to avoid error log entry
      conn.WriteToUDP([]byte{0,4,0,1}, remote_addr)
    }
    
    _,err = conn.WriteToUDP([]byte("\000\001foo-wAfFel\000octet\000"),tftp_addr)
    check(err,nil)
    conn.SetReadDeadline(time.Now().Add(3*time.Second))
    n, remote_addr, err = conn.ReadFromUDP(buf)
    check(err,nil)
    if check(n >= 4, true) {
      check(buf[0:4], []byte{0,3,0,1})
      check(string(buf[4:n]), "00:00:00:00:af:fe\nwAfFel\n2\nfox\nhound\nfoo-wAfFel\n")
      // send ACK to avoid error log entry
      conn.WriteToUDP([]byte{0,4,0,1}, remote_addr)
    }
    
    _,err = conn.WriteToUDP([]byte("\000\001foo-33000011110000\000octet\000"),tftp_addr)
    check(err,nil)
    conn.SetReadDeadline(time.Now().Add(3*time.Second))
    n, remote_addr, err = conn.ReadFromUDP(buf)
    check(err,nil)
    if check(n >= 4, true) {
      check(buf[0:4], []byte{0,3,0,1})
      check(string(buf[4:n]), "00:00:11:11:00:00\n33000011110000\n2\nfoxnotebook-template\nhound\nfoo-33000011110000\n")
      // send ACK to avoid error log entry
      conn.WriteToUDP([]byte{0,4,0,1}, remote_addr)
    }
  }
}

func run_trigger_activate_new_tests() {
  // 1) send jtan for non-existing system (with timestamp in the future)
  //    with ogroup="Desktops" and no base
  //    * has install job been created with correct properties (timestamp, MAC)
  //    * has LDAP object been created in ou=workstations,ou=systems,o=go-susi,c=de
  //      - with the correct attributes (matching those from desktop-template)
  //    * ipHostNumber filled in properly?
  //    * send here_i_am with local IP
  //    * check that system has been renamed and IP number has been updated
  //    * clear jobs and delete LDAP object
  //
  // 2) send jtan for non-existing system (with timestamp in the future)
  //    with ogroup="www.mit.edu" and no <base> and no <ip>
  //    * has install job been created with correct properties (timestamp, MAC)
  //    * has LDAP object been created in ou=servers,ou=systems,o=go-susi,c=de
  //      - with the correct attributes (matching those from www.mit.edu)
  //    * ipHostNumber empty or non-existing?
  //    * clear jobs and delete LDAP object
  //
  // 3) create LDAP entry "schlumpf" in incoming as copy of "www.mit.edu" with
  //    different MAC. Set gotoMode to "locked".
  //    send jtan for non-existing system (with timestamp in the future)
  //    with ogroup="schlumpf" and no <base> and no <ip>
  //    * has install job been created with correct properties (timestamp, MAC)
  //    * has LDAP object been created in ou=incoming,o=go-susi,c=de
  //      - with the correct attributes (matching those from schlumpf)
  //    * ipHostNumber empty or non-existing
  //    * gotoMode of the new object is active
  //    * clear jobs and delete the auto-created LDAP-object but keep schlumpf
  //
  // 4) send jtan for schlumpf with base="o=go-susi,c=de"
  //    * check that schlumpf has been moved to ou=servers,ou=systems,o=go-susi,c=de
  //    * delete schlumpf
  //    * clear jobs and delete LDAP object
  //
  // 5) send jtan without ogroup and without base
  //    * check that LDAP object has been created in ou=incoming,o=go-susi,c=de
  //    * gotoMode active
  //    * clear jobs and delete LDAP object

  // clear jobdb  
  gosa("delete_jobdb_entry", hash("xml(where())"))
  time.Sleep(reply_timeout)

  mac := "fa:1e:c9:76:00:aa"
  ts := "20200102030405"
  
  // make sure there's no old entry from a (crashed) previous run
  sys,_ := db.SystemGetAllDataForMAC(mac, false)
  db.SystemReplace(sys, nil)
  
  gosa("job_trigger_activate_new", hash("xml(timestamp(%v)mac(%v)ogroup(Desktops)ip(1.2.3.44))",ts,mac))
  jtan(mac, ts, "ou=workstations,ou=systems,o=go-susi,c=de", "desktop-template", "1.2.3.44")
  sysold,err := db.SystemGetAllDataForMAC(mac, false)
  check(err,nil)
  
  db.SystemSetStateMulti(mac, "iphostnumber", []string{})
  hia := hash("xml(header(here_i_am)source(%v)target(%v)new_passwd(%v)mac_address(%v))", client_listen_address, config.ServerSourceAddress, keys[len(keys)-1], mac)
  t0 := time.Now()
  send("[ClientPackages]", hia)
  waitlong(t0, "registered")
  
  time.Sleep(2*time.Second) // give server time to update ipHostNumber in the LDAP entry
  
  sysnew,err := db.SystemGetAllDataForMAC(mac, false)
  if check(err, nil) {
    check(sysnew.Text("cn"), "_fa-1e-c9-76-00-aa_")
    check(sysnew.Text("dn"), "cn=_fa-1e-c9-76-00-aa_,ou=workstations,ou=systems,o=go-susi,c=de")
    sysold.RemoveFirst("cn")
    sysold.RemoveFirst("dn")
    sysold.FirstOrAdd("iphostnumber").SetText(strings.SplitN(client_listen_address,":",2)[0])
    sysnew.RemoveFirst("cn")
    sysnew.RemoveFirst("dn")
    // faistate is affected by side-effects not part of the test (deleting a job sets faistate to "localboot"), so we remove it
    sysnew.RemoveFirst("faistate")
    sysold.RemoveFirst("faistate")
    check(sysold, sysnew)
  }
  sys,_ = db.SystemGetAllDataForMAC(mac, false)
  db.SystemReplace(sys, nil)
  
  gosa("job_trigger_activate_new", hash("xml(timestamp(%v)mac(%v)ogroup(www.mit.edu))",ts,mac))
  jtan(mac, ts, "ou=servers,ou=systems,o=go-susi,c=de", "www.mit.edu", "")
  sys,_ = db.SystemGetAllDataForMAC(mac, false)
  db.SystemReplace(sys, nil)
  
  schlumpf,_ := db.SystemGetAllDataForMAC(db.SystemMACForName("www.mit.edu"),false)
  schlumpf.First("cn").SetText("schlumpf")
  schlumpf.First("dn").SetText("cn=schlumpf,ou=incoming,o=go-susi,c=de")
  schlumpf.FirstOrAdd("gotomode").SetText("locked")
  schlumpf.First("macaddress").SetText("1e:22:ff:00:00:99")
  db.SystemReplace(nil, schlumpf)
  gosa("job_trigger_activate_new", hash("xml(timestamp(%v)mac(%v)ogroup(schlumpf))",ts,mac))
  jtan(mac, ts, "ou=incoming,o=go-susi,c=de", "schlumpf", "")
  sys,_ = db.SystemGetAllDataForMAC(mac, false)
  db.SystemReplace(sys, nil)
  
  gosa("job_trigger_activate_new", hash("xml(timestamp(%v)mac(%v)base(o=go-susi,c=de))",ts,schlumpf.Text("macaddress")))
  jtan(schlumpf.Text("macaddress"), ts, "ou=servers,ou=systems,o=go-susi,c=de", "www.mit.edu", "18.9.22.169")
  schlumpf,_ = db.SystemGetAllDataForMAC(schlumpf.Text("macaddress"), false)
  db.SystemReplace(schlumpf, nil)
  
  gosa("job_trigger_activate_new", hash("xml(timestamp(%v)mac(%v))",ts,mac))
  jtan(mac, ts, "ou=incoming,o=go-susi,c=de", "", "")
  sys,_ = db.SystemGetAllDataForMAC(mac, false)
  db.SystemReplace(sys, nil)
}

// Checks that a job_trigger_activate_new has been properly executed, then deletes
// the LDAP entry with macAdress=mac and the jobdb.
// template_sys_name is the cn of a system object to compare the generated object with.
func jtan(mac, timestamp, ou, template_sys_name, ip string) {
  _, file, line, _ := runtime.Caller(1)
  file = file[strings.LastIndex(file, "/")+1:]
  fmt.Printf("== job_trigger_activate_new sub-tests (%v:%v) ==\n", file, line)
  
  // wait for server to process job_trigger_activate_new message
  time.Sleep(reply_timeout)
  
  job := gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v)))clause(phrase(timestamp(%v)))clause(phrase(headertag(trigger_action_reinstall)))))",mac,timestamp))
  check(checkTags(job, "header,source,target,session_id?,answer1"),"")
  sys, err := db.SystemGetAllDataForMAC(mac, false)
  if check(err, nil) {
    check(checkTags(sys,"dn,cn,macaddress,objectclass+,gotoxmousetype?,gotontpserver?,gotoldapserver?,gotosysstatus?,gotoxdriver?,gotoxhsync?,gotoxvsync?,gotoxresolution?,gotoxkbmodel?,gotomodules*,ghmemsize?,gotoxmonitor?,fairepository*,ghusbsupport?,gotomode,iphostnumber?,gotolastuser?,gotohardwarechecksum?,gosaunittag?,faiclass?,ghsoundadapter?,ghgfxadapter?,gotoxkbvariant?,l?,gotoxcolordepth?,ghnetnic?,gotoxmouseport?,gotobootkernel?,ghcputype?,faidebianmirror?,ghscsidev?,gofonhardware?,gotosndmodule?,ghidedev?,description?,faistate?,gotoxkblayout?"),"")
    check(strings.SplitN(sys.Text("dn"),",",2)[1],ou)
    check(sys.Text("macaddress"),mac)
    check(sys.Text("iphostnumber"),ip)
    check(sys.Text("gotomode"), "active")
    check(hasWords(sys.Text("objectclass","GOhard")),"")
    if template_sys_name != "" {
      template,_ := db.SystemGetAllDataForMAC(db.SystemMACForName(template_sys_name), false)
      oc1 := sys.Get("objectclass")
      sort.Strings(oc1)
      oc2 := template.Get("objectclass")
      sort.Strings(oc2)
      check(oc1, oc2)  
      check(sys.Text("gosaunittag"), template.Text("gosaunittag"))
      check(sys.Text("gotoxdriver"), template.Text("gotoxdriver"))
      check(sys.Text("gotoxresolution"), template.Text("gotoxresolution"))
      check(sys.Text("faidebianmirror"), template.Text("faidebianmirror"))
      check(sys.Text("gotontpserver"), template.Text("gotontpserver"))
      check(sys.Text("gotobootkernel"), template.Text("gotobootkernel"))
      check(sys.Text("gotoldapserver"), template.Text("gotoldapserver"))
      check(sys.Text("faiclass"), template.Text("faiclass"))
      check(sys.Text("fairepository"), template.Text("fairepository"))
      
      grp1 := db.SystemGetGroupsWithMember(sys.Text("dn"))
      grp2 := db.SystemGetGroupsWithMember(template.Text("dn"))
      check(grp1, grp2)
    }
  }
  
  // clear jobdb
  gosa("delete_jobdb_entry", hash("xml(where())"))
}

func run_save_fai_log_tests() {
  mac := Jobs[1].MAC
  lowermac := strings.ToLower(mac)
  check(mac != lowermac, true)
  
  x := gosa("show_log_by_mac", hash("xml(mac(%v))", mac))
  check(checkTags(x,"header,source,target,show_log_by_mac,session_id?"), "")
  
  x = gosa("show_log_files_by_date_and_mac", hash("xml(mac(%v)date(install_20120102160304))", mac))
  check(checkTags(x,"header,source,target,show_log_files_by_date_and_mac,session_id?"), "")
  check(x.Get("show_log_files_by_date_and_mac"),[]string{""})
  
  x = hash("xml(header(CLMSG_save_fai_log)source(%v)target(%v)macaddress(%v)fai_action(install))",client_listen_address, config.ServerSourceAddress, mac)
  x.Add("CLMSG_save_fai_log", fmt.Sprintf("log_file:frodo:SG9i Yml0 Cg== log_file:gandalf:V2\nl6YXJkCg==\n"))
  send("CLIENT", x)
  time.Sleep(1*time.Second)
  ls, err := ioutil.ReadDir(path.Join(confdir,lowermac))
  if check(err, nil) && check(len(ls), 1) {
    logdir := path.Join(confdir, lowermac, ls[0].Name())
    logdata,err := ioutil.ReadFile(logdir+"/frodo")
    if check(err,nil) {
      check(string(logdata),"Hobbit\n")
    }
    logdata,err = ioutil.ReadFile(logdir+"/gandalf")
    if check(err,nil) {
      check(string(logdata),"Wizard\n")
    }
  }
  
  // Check if convenience symlink exists
  target, err := os.Readlink(path.Join(confdir, Jobs[1].Plainname))
  check(err, nil)
  check(target, lowermac)
  
  ele := "mac_"+strings.Replace(lowermac,":","_",-1)
  x = gosa("show_log_by_mac", hash("xml(mac(%v))", mac))
  if check(checkTags(x,"header,source,target,show_log_by_mac,session_id?,"+ele), "") {
    dirname := x.Text(ele)
    t := util.ParseTimestamp(strings.Replace(strings.Replace(dirname,"_","",-1), "install","",-1))
    dur := time.Since(t)
    if dur <= 3*time.Second { dur = 0 }
    check(dur, time.Duration(0))
    
    x = gosa("show_log_files_by_date_and_mac", hash("xml(mac(%v)date(%v))", mac, dirname))
    if check(checkTags(x,"header,source,target,show_log_files_by_date_and_mac+,session_id?"), "") {
      check(x.Get("show_log_files_by_date_and_mac"),[]string{"","frodo","gandalf"})
    }
    
    x = gosa("get_log_file_by_date_and_mac", hash("xml(mac(%v)date(%v)log_file(%v))", mac, dirname, "frodo"))
    if check(checkTags(x,"header,source,target,session_id?,get_log_file_by_date_and_mac,frodo"), "") {
      x.First("frodo").DecodeBase64()
      check(x.Text("frodo"), "Hobbit\n")
    }
    
    x = gosa("get_log_file_by_date_and_mac", hash("xml(mac(%v)date(%v)log_file(%v))", mac, dirname, "gandalf"))
    if check(checkTags(x,"header,source,target,session_id?,get_log_file_by_date_and_mac,gandalf"), "") {
      x.First("gandalf").DecodeBase64()
      check(x.Text("gandalf"), "Wizard\n")
    }
  }
}

func run_hook_tests() {
  // createConfigFile() has already generated hook scripts and go-susi
  // should have run them once right after starting. So we start by
  // checking if the initial data corresponds to what the hook scripts
  // from createConfigFile() output

  x := gosa("get_available_kernel", hash("xml(fai_release(ignaz))"))
  if check(x.Text("header"), "get_available_kernel") {
    answers := extract_sorted_answers(x)
    check(answers.Get("answer"),[]string{"michael","tobi"})
  }
  
  x = gosa("get_available_kernel", hash("xml(fai_release(dennis))"))
  if check(x.Text("header"), "get_available_kernel") {
    answers := extract_sorted_answers(x)
    check(answers.Get("answer"),[]string{"jan-marek"})
  }
  
  x = gosa("get_available_kernel", hash("xml(fai_release(matthias))"))
  if check(x.Text("header"), "get_available_kernel") {
    answers := extract_sorted_answers(x)
    check(answers.Get("answer"),[]string{})
  }
  
  x = gosa("get_available_kernel", hash("xml(fai_release(chef))"))
  if check(x.Text("header"), "get_available_kernel") {
    answers := extract_sorted_answers(x)
    check(answers.Get("answer"),[]string{})
  }
  
  
  x = gosa("query_packages_list", hash("xml(where(clause(phrase(distribution(kuschel)))))"))
  if check(x.Text("header"), "query_packages_list") {
    answers := extract_sorted_answers(x)
    
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,distribution,package,version,section,description,template"),"") {
      check(a.Text("distribution"),"kuschel")
      check(a.Text("package"),"baer")
      check(a.Text("template"),"")
      a = a.Next()
    }
    
    if check(checkTags(a,"timestamp,distribution,package,version,section,description,template"),"") {
      check(a.Text("distribution"),"kuschel")
      check(a.Text("package"),"faultier")
      decoded,err := base64.StdEncoding.DecodeString(a.Text("description"))
      check(err,nil)
      check(string(decoded),"knuddelig und langsam")
      check(a.Text("version"),"9.8")
      check(a.Text("section"),"tree")
      decoded,err = base64.StdEncoding.DecodeString(a.Text("template"))
      check(err,nil)
      check(string(decoded),"foo")
      a = a.Next()
    }
    
    if check(checkTags(a,"timestamp,distribution,package,version,section,description,template"),"") {
      check(a.Text("distribution"),"kuschel")
      check(a.Text("package"),"otter")
      decoded,err := base64.StdEncoding.DecodeString(a.Text("description"))
      check(err,nil)
      check(string(decoded),"schwimmtier")
      check(a.Text("version"),"3.5")
      check(a.Text("section"),"wasser")
      decoded,err = base64.StdEncoding.DecodeString(a.Text("template"))
      check(err,nil)
      check(string(decoded),"spezi")
      a = a.Next()
    }
    
    check(a, nil)
  }
  
  x = gosa("query_packages_list", hash("xml(where(clause(phrase(distribution(pluesch)))))"))
  if check(x.Text("header"), "query_packages_list") {
    answers := extract_sorted_answers(x)
    
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,distribution,package,version,section,description,template"),"") {
      check(a.Text("distribution"),"pluesch")
      check(a.Text("package"),"otter")
      decoded,err := base64.StdEncoding.DecodeString(a.Text("description"))
      check(err,nil)
      check(string(decoded),"schwimmtier")
      check(a.Text("version"),"3.5")
      check(a.Text("section"),"wasser")
      decoded,err = base64.StdEncoding.DecodeString(a.Text("template"))
      check(err,nil)
      check(string(decoded),"spezi")
      a = a.Next()
    }
    
    check(a, nil)
  }
  
  x = gosa("query_packages_list", hash("xml(where(clause(phrase(distribution(flausch)))))"))
  if check(x.Text("header"), "query_packages_list") {
    answers := extract_sorted_answers(x)
    
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,distribution,package,version,section,description,template"),"") {
      check(a.Text("distribution"),"flausch")
      check(a.Text("package"),"otter")
      decoded,err := base64.StdEncoding.DecodeString(a.Text("description"))
      check(err,nil)
      check(string(decoded),"schwimmtier")
      check(a.Text("version"),"3.5")
      check(a.Text("section"),"wasser")
      decoded,err = base64.StdEncoding.DecodeString(a.Text("template"))
      check(err,nil)
      check(string(decoded),"spezi")
      a = a.Next()
    }
    
    check(a, nil)
  }
  
  x = gosa("query_packages_list", hash("xml(where(clause(phrase(distribution(doux)))))"))
  if check(x.Text("header"), "query_packages_list") {
    answers := extract_sorted_answers(x)
    
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,distribution,package,version,section,description,template"),"") {
      check(a.Text("distribution"),"doux")
      check(a.Text("package"),"otter")
      decoded,err := base64.StdEncoding.DecodeString(a.Text("description"))
      check(err,nil)
      check(string(decoded),"wassertier")
      check(a.Text("version"),"3.6")
      check(a.Text("section"),"fluss")
      decoded,err = base64.StdEncoding.DecodeString(a.Text("template"))
      check(err,nil)
      check(string(decoded),"paulaner")
      a = a.Next()
    }
    
    check(a, nil)
  }
  
  x = gosa("query_packages_list", hash("xml(where(clause(phrase(distribution(weich)))))"))
  if check(x.Text("header"), "query_packages_list") {
    answers := extract_sorted_answers(x)
    
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,distribution,package,version,section,description,template"),"") {
      check(a.Text("distribution"),"weich")
      check(a.Text("package"),"otter")
      decoded,err := base64.StdEncoding.DecodeString(a.Text("description"))
      check(err,nil)
      check(string(decoded),"wassertier")
      check(a.Text("version"),"3.6")
      check(a.Text("section"),"fluss")
      decoded,err = base64.StdEncoding.DecodeString(a.Text("template"))
      check(err,nil)
      check(string(decoded),"paulaner")
      a = a.Next()
    }
    
    check(a, nil)
  }
  
  x = gosa("query_packages_list", hash("xml(where(clause(phrase(version(3.6)))))"))
  if check(x.Text("header"), "query_packages_list") {
    answers := extract_sorted_answers(x)
    
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,distribution,package,version,section,description,template"),"") {
      check(a.Text("distribution"),"soft")
      check(a.Text("package"),"otter")
      decoded,err := base64.StdEncoding.DecodeString(a.Text("description"))
      check(err,nil)
      check(string(decoded),"wassertier")
      check(a.Text("version"),"3.6")
      check(a.Text("section"),"fluss")
      decoded,err = base64.StdEncoding.DecodeString(a.Text("template"))
      check(err,nil)
      check(string(decoded),"paulaner")
      a = a.Next()
    }
    
    check(a, nil)
  }
  
  x = gosa("query_packages_list", hash("xml(where(clause(phrase(distribution(schmusen)))))"))
  if check(x.Text("header"), "query_packages_list") {
    answers := extract_sorted_answers(x)
    check(answers.Get("answer"),[]string{})
  }
  
  
  // Now we replace the hook scripts and send SIGUSR2, causing go-susi
  // to run the new hooks. Then we check for the respective data.
  
  ioutil.WriteFile(generate_kernel_list, []byte(`#!/bin/bash
echo "release: matthias"
echo "cn: matze"
echo
echo "cn: benki"
echo "release: matthias"
echo
echo "release: matthias"
echo "cn: brownie"
echo
echo "release: chef"
echo "cn: matthias"
echo
echo "release: chef"
echo "cn: andrea"
`), 0755)

  ioutil.WriteFile(generate_package_list, []byte(`#!/bin/bash
echo "
Release: schmusen
Package: hund
Version: weich
Section: sofa
Description: lieb
Template: hundkatzemaus
"
`), 0755)

  daemonProcess.Signal(syscall.SIGUSR2)
  time.Sleep(2*time.Second)
  
  x = gosa("get_available_kernel", hash("xml(fai_release(matthias))"))
  if check(x.Text("header"), "get_available_kernel") {
    answers := extract_sorted_answers(x)
    check(answers.Get("answer"),[]string{"benki","brownie","matze"})
  }
  
  x = gosa("get_available_kernel", hash("xml(fai_release(chef))"))
  if check(x.Text("header"), "get_available_kernel") {
    answers := extract_sorted_answers(x)
    check(answers.Get("answer"),[]string{"andrea","matthias"})
  }
  
  
  x = gosa("query_packages_list", hash("xml(where(clause(phrase(distribution(schmusen)))))"))
  if check(x.Text("header"), "query_packages_list") {
    answers := extract_sorted_answers(x)
    
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,distribution,package,version,section,description,template"),"") {
      check(a.Text("distribution"),"schmusen")
      check(a.Text("package"),"hund")
      decoded,err := base64.StdEncoding.DecodeString(a.Text("description"))
      check(err,nil)
      check(string(decoded),"lieb")
      check(a.Text("version"),"weich")
      check(a.Text("section"),"sofa")
      decoded,err = base64.StdEncoding.DecodeString(a.Text("template"))
      check(err,nil)
      check(string(decoded),"hundkatzemaus")
      a = a.Next()
    }
    
    check(a, nil)
  }
  
  x = gosa("query_packages_list", hash("xml(where(clause(phrase(distribution(kuschel)))))"))
  if check(x.Text("header"), "query_packages_list") {
    answers := extract_sorted_answers(x)
    check(answers.Get("answer"),[]string{})
  }
  
  
  x = gosa("job_send_user_msg", hash("xml(timestamp(%v)macaddress(GOSA)foo(bar))", util.MakeTimestamp(time.Now())))
  if check(x.Text("header"), "answer") {
    time.Sleep(1*time.Second) // wait for hook script to have run.
    data, err := ioutil.ReadFile(confdir+"/send_user_msg.env")
    check(err, nil)
    check(hasWords(string(data), "xml='<xml><foo>bar</foo><headertag>send_user_msg</headertag>", "foo=bar"), "")
  }
}

func run_fai_query_tests() {
  x := gosa("query_fai_server", hash("xml()"))
  if check(x.Text("header"), "query_fai_server") {
    answers := extract_sorted_answers(x)
    
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,repopath,fai_release,tag,server,sections"),"") {
      check(a.Text("fai_release"),"knochen/2.0")
      check(a.Text("tag"),"99")
      check(a.Text("server"),"http://strolchi/liebtsusi")
      check(a.Text("repopath"),"knochen/2.0")
      check(a.Text("sections"),"knackig,crunchy")
      a = a.Next()
    }
    
    if check(checkTags(a,"timestamp,repopath,fai_release,tag,server,sections"),"") {
      check(a.Text("fai_release"),"plophos/4.1.0")
      check(a.Text("tag"),"7")
      check(a.Text("server"),"http://www.mit.edu/repo")
      check(a.Text("repopath"),"plophos/4.1.0")
      check(a.Text("sections"),"main,restricted,universe,multiverse")
      a = a.Next()
    }
    
    if check(checkTags(a,"timestamp,repopath,fai_release,tag,server,sections"),"") {
      check(a.Text("fai_release"),"plophos")
      check(a.Text("tag"),"99")
      check(a.Text("server"),"http://www.mit.edu/repo2")
      check(a.Text("repopath"),"plophos")
      check(a.Text("sections"),"dusch,gel")
      a = a.Next()
    }
    
    if check(checkTags(a,"timestamp,repopath,fai_release,tag,server,sections"),"") {
      check(a.Text("fai_release"),"tramp/5.0.0")
      check(a.Text("tag"),"99")
      check(a.Text("server"),"http://www.mit.edu/repo3")
      check(a.Text("repopath"),"tramp/5.0.0")
      check(a.Text("sections"),"cool,super,geil")
      a = a.Next()
    }

    check(a, nil)
  }
  
  /////////////////////////////////// xavier //////////////////////////////////////
  x = gosa("query_fai_release", hash("xml(where(clause(phrase(fai_release(xavier)))))"))
  if check(x.Text("header"), "query_fai_release") {
    answers := extract_sorted_answers(x)
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,fai_release,tag,type,class,state"),"") {
      check(a.Text("fai_release"),"xavier")
      check(a.Text("class"),"TURTLE")
      check(a.Text("state"),"freeze")
      check(a.Text("tag"),"7")
      check(a.Text("type"),"FAIpartitionTable")
      
      a = a.Next()
    }
    
    if check(checkTags(a,"timestamp,fai_release,tag,type,class,state"),"") {
      check(a.Text("fai_release"),"xavier")
      check(a.Text("class"),"WORLD")
      check(a.Text("state"),"freeze")
      check(a.Text("tag"),"7")
      check(a.Text("type"),"FAIpartitionTable")
      
      a = a.Next()
    }

    check(a, nil)
  }
  
  /////////////////////////////////// xavier/charles //////////////////////////////////////
  x = gosa("query_fai_release", hash("xml(where(clause(phrase(fai_release(xavier/charles)))))"))
  if check(x.Text("header"), "query_fai_release") {
    answers := extract_sorted_answers(x)
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,fai_release,tag,type,class,state"),"") {
      check(a.Text("fai_release"),"xavier/charles")
      check(a.Text("class"),"WORLD")
      check(a.Text("state"),"freeze")
      check(a.Text("tag"),"7")
      check(a.Text("type"),"FAIpartitionTable")
      
      a = a.Next()
    }

    check(a, nil)
  }
  
  /////////////////////////////////// xavier/charles/prof //////////////////////////////////////
  x = gosa("query_fai_release", hash("xml(where(clause(phrase(fai_release(xavier/charles/prof)))))"))
  if check(x.Text("header"), "query_fai_release") {
    answers := extract_sorted_answers(x)
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,fai_release,tag,type,class,state"),"") {
      check(a.Text("fai_release"),"xavier/charles/prof")
      check(a.Text("class"),"TURTLE")
      check(a.Text("state"),"")
      check(a.Text("tag"),"7")
      check(a.Text("type"),"FAIpartitionTable")
      
      a = a.Next()
    }
    
    if check(checkTags(a,"timestamp,fai_release,tag,type,class,state"),"") {
      check(a.Text("fai_release"),"xavier/charles/prof")
      check(a.Text("class"),"WORLD")
      check(a.Text("state"),"")
      check(a.Text("tag"),"7")
      check(a.Text("type"),"FAIpartitionTable")
      
      a = a.Next()
    }

    check(a, nil)
  }
  
  /////////////////////////////////// xavier/charles/prof/x-men //////////////////////////////////////
  x = gosa("query_fai_release", hash("xml(where(clause(phrase(fai_release(xavier/charles/prof/x-men)))))"))
  if check(x.Text("header"), "query_fai_release") {
    answers := extract_sorted_answers(x)
    a := answers.First("answer")
    
    if check(checkTags(a,"timestamp,fai_release,tag,type,class,state"),"") {
      check(a.Text("fai_release"),"xavier/charles/prof/x-men")
      check(a.Text("class"),"TURTLE")
      check(a.Text("state"),"")
      check(a.Text("tag"),"7")
      check(a.Text("type"),"FAIpartitionTable")
      
      a = a.Next()
    }
    
    if check(checkTags(a,"timestamp,fai_release,tag,type,class,state"),"") {
      check(a.Text("fai_release"),"xavier/charles/prof/x-men")
      check(a.Text("class"),"WORLD")
      check(a.Text("state"),"")
      check(a.Text("tag"),"7")
      check(a.Text("type"),"FAIpartitionTable")
      
      a = a.Next()
    }

    check(a, nil)
  }
}


func run_detected_hardware_tests() {
  /*
    TEST 1: New system sends here_i_am, then detected_hardware, no templates match
    send here_i_am for our client_listen_address with a MAC for which there is no LDAP entry
    wait for the detect_hardware message from the server
    Send detected_hardware with
      * two <detected_hardware> elements
      * multiple <gotoModules> sub-elements in both <detected_hardware> elements
        with different capitalizations of the word "GoTOmodulEs"
      * ghMemSize and gotoSndModule as XML attributes
      * gotoxdriver="notemplate" to make sure we don't accidentally match a template
        due to the name of the machine the test is being run on
      * gotoLdapServer
    wait a bit for LDAP to be updated
    check LDAP for the new entry (use checkTags!)
    check in particular cn (should start with config.Hostname) and dn (under ou=incoming)
    check that we did not receive a set_activated_for_installation message
    check that we did receive ldap information
  */
  mac := "aa:00:bb:11:cc:99"
  hia := hash("xml(header(here_i_am)source(%v)target(%v)new_passwd(%v)mac_address(%v))", client_listen_address, config.ServerSourceAddress, keys[len(keys)-1], mac)
  t0 := time.Now()
  send("[ClientPackages]", hia)
  check(waitlong(t0, "detect_hardware").XML.Text("header"),"detect_hardware")
  detected_hardware := hash("xml(header(detected_hardware)source(%v))",client_listen_address)
  dh := hash("detected_hardware(gotoModules(m1)goTOModules(m2)gotoxdriver(notemplate))")
  detected_hardware.AddWithOwnership(dh)
  dh2 := "<detected_hardware ghMemSize='12345' gotoLdapServer='1:ldap01.tvc.example.com:ldap://ldap01.tvc.example.com/o=go-susi,c=de' gotoSndModule=\"snd_noisemaster\"><gotoMODULES>m3</gotoMODULES><GOTOModulES>m4</GOTOModulES></detected_hardware>"
  dh_string := strings.Replace(detected_hardware.String(),"<detected_hardware>",dh2+"<detected_hardware>",-1)
  t0 = time.Now()
  util.SendLnTo(config.ServerSourceAddress, security.GosaEncrypt(dh_string, keys[len(keys)-1]), config.Timeout)
  check(waitlong(t0, "set_activated_for_installation").XML,"<xml></xml>")
  check(waitlong(t0, "new_ldap_config").XML.Text("ldap_uri"), "ldap://ldap01.tvc.example.com")
  sys,err := db.SystemGetAllDataForMAC(mac, false)
  if check(err,nil) {
    check(checkTags(sys,"dn,cn,faistate,macaddress,gotoldapserver,objectclass+,gotosysstatus?,gotoxdriver,gotomodules+,ghmemsize,gotosndmodule,gotomode,iphostnumber"),"")
    check(hasWords(sys.Text("cn"),config.Hostname),"")
    check(hasWords(sys.Text("dn"),"cn="+config.Hostname,"ou=incoming,"+config.LDAPBase),"")
    check(sys.Text("faistate"),"install")
    check(sys.Text("macaddress"),mac)
    check(sys.Text("iphostnumber"),config.IP)
    modules := sys.Get("gotomodules")
    sort.Strings(modules)
    check(modules,[]string{"m1","m2","m3","m4"})
    check(sys.Text("ghmemsize"),"12345")
    check(sys.Text("gotosndmodule"),"snd_noisemaster")
    check(sys.Text("gotoxdriver"),"notemplate")
    check(sys.Text("gotomode"),"locked")
    oc := sys.Get("objectclass")
    sort.Strings(oc)
    check(oc, []string{"FAIobject","GOhard"})
  }
  /*  
    TEST 2: System sends updated hardware information
    Send detected_hardware with
      * changed ghMemSize with different capitalization of "GhMemSIZe"
      * empty <gotoSndModule></gotoSndModule>
      * added gotoXMouseType
      * one new and one removed <gotoModules> element
      * gotoMode "active"
      * no gotoxdriver="notemplate" (should be unnecessary because
                  templates should not be applied because object exists)
    wait a bit for LDAP to be updated
    check that we do receive set_activated_for_installation message (because of gotoMode "active")
    check that object has been updated (check complete object, checkTags!)
    delete system from LDAP
  */
  dh = detected_hardware.First("detected_hardware")
  dh.Add("GhMemSIZe","1976")
  dh.Add("gotoSndModule")
  dh.Add("gotoModules","m0")
  dh.Add("gotoModules","m4")
  dh.Add("gotoXMouseType","FetteRatte")
  dh.Add("gotoMode","active")
  dh.Add("gotoLdapServer","1:ldap01.tvc.example.com:ldap://ldap01.tvc.example.com/o=go-susi")
  dh.Add("gotoNtpServer","ntp.example.com")
  dh.RemoveFirst("gotoxdriver")
  t0 = time.Now()
  send("CLIENT", detected_hardware)
  
  check(waitlong(t0, "set_activated_for_installation").IsClientMessage, true)
  check(waitlong(t0, "new_ldap_config").XML.Text("ldap_uri"), "ldap://ldap01.tvc.example.com")
  check(waitlong(t0, "new_ntp_config").XML.Text("server"), "ntp.example.com")
  
  sys, err = db.SystemGetAllDataForMAC(mac, false)
  if check(err,nil) {
    check(checkTags(sys,"dn,cn,macaddress,faistate,objectclass+,gotoxmousetype,gotontpserver,gotoldapserver,gotosysstatus?,gotoxdriver,gotomodules+,ghmemsize,gotomode,iphostnumber"),"")
    check(hasWords(sys.Text("cn"),config.Hostname),"")
    check(hasWords(sys.Text("dn"),"cn="+config.Hostname,"ou=incoming,"+config.LDAPBase),"")
    check(sys.Text("macaddress"),mac)
    check(sys.Text("iphostnumber"),config.IP)
    modules := sys.Get("gotomodules")
    sort.Strings(modules)
    check(modules,[]string{"m0","m1","m2","m4"})
    check(sys.Text("ghmemsize"),"1976")
    check(sys.Text("gotomode"),"active")
    check(sys.Text("gotoxmousetype"),"FetteRatte")
    check(sys.Text("gotoxdriver"),"notemplate")
    oc := sys.Get("objectclass")
    sort.Strings(oc)
    check(oc, []string{"FAIobject","GOhard"})
  }
  
  if sys != nil { 
    err = db.SystemReplace(hash("xml(dn(cn=%v,ou=incoming,%v))",sys.Text("cn"),config.LDAPBase), nil) 
    if err != nil {
      fmt.Printf("ERROR! Could not delete test system. Manual cleanup of testdata/ldif may be necessary! LDAP error: %v\n", err)
    }
  }
  
  /*
    TEST 3: New system with matching template object in object group; override IP
    Send detected_hardware with
      * ipHostNumber element with IP that can't be resolved to a name
      * ghcputype that includes "GenuineIntel / Intel" so that desktop-template matches
      * gotoxresolution=1280x1024
    wait a bit for LDAP to be updated
    Check that we don't receive set_activated_for_installation message
    Check LDAP for new entry
    Check that generated cn is system-<MAC_with_minus_instead_of_colon>
    Check that gotoxresolution, ghcputype and ipHostNumber come from detected_hardware
        but the rest comes from the template object
    Check that system is member of object group Desktops
    Check that gocomment and description are not copied from the template object
    Check objectClass=gosaAdministrativeUnitTag and gosaUnitTag
    Check that system is under ou=workstations,ou=systems instead of ou=incoming
  */
  
  detected_hardware = hash("xml(header(detected_hardware)source(%v))",client_listen_address)
  dh = hash("detected_hardware(ipHostNumber(10.255.255.255)gotoxresolution(1280x1024)ghCPUType(GenuineIntel  /  Intel Celery))")
  detected_hardware.AddWithOwnership(dh)
  t0 = time.Now()
  send("CLIENT", detected_hardware)
  
  check(waitlong(t0, "set_activated_for_installation").XML, "<xml></xml>")
  
  test3_result := xml.NewHash("xml")
  sys, err = db.SystemGetAllDataForMAC(mac, false)
  if check(err,nil) {
    test3_result = sys.Clone()
    check(checkTags(sys,"dn,cn,faistate,macaddress,objectclass+,gotosysstatus?,gotomode,iphostnumber,ghcputype,ghgfxadapter,ghmemsize,ghsoundadapter,ghusbsupport,gofonhardware,gosaunittag,gotolastuser,gotoxcolordepth,gotoxresolution,gotoxdriver,gotoxkblayout,gotoxkbmodel,gotoxkbvariant,gotoxmousetype,gotoxmouseport"),"")
    check(sys.Text("cn"),"_aa-00-bb-11-cc-99_")
    check(hasWords(sys.Text("dn"),"cn="+sys.Text("cn"),"ou=workstations,ou=systems,"+config.LDAPBase),"")
    check(sys.Text("macaddress"),mac)
    check(sys.Text("iphostnumber"),"10.255.255.255")
    check(sys.Text("ghmemsize"),"-")
    check(sys.Text("gotomode"),"locked")
    check(sys.Text("gosaunittag"),"7")
    check(sys.Text("ghcputype"),"GenuineIntel  /  Intel Celery")
    check(sys.Text("gotoxresolution"),"1280x1024")
    oc := sys.Get("objectclass")
    sort.Strings(oc)
    check(oc, []string{"FAIobject","GOhard","gosaAdministrativeUnitTag","gotoWorkstation","top"})
    groups :=db.SystemGetGroupsWithMember(sys.Text("dn"))
    if check(groups.First("xml")!=nil, true) {
      check(groups.First("xml").Next(),nil)
      check(groups.First("xml").Text("cn"),"Desktops")
    }
  }
  
  /*
    TEST 4: Use detected_hardware to change cn
    Send detected_hardware with
      * macAddress attribute(not sub-element!) of <detected_hardware> element
      * ipHostNumber attribute with different IP
      * missing <source> element
      * cn=mrhyde
      * no dn
      * encrypted with [GOsaPackages] key
    Wait a bit for LDAP to be updated
    Check that object is the same as after TEST 3 except for
      * ipHostNumber
      * cn
      * dn matching cn
  */
  
  t0 = time.Now()
  dh_string = "<xml><header>detected_hardware</header><detected_hardware macAddress='"+mac+"' ipHostNumber='"+config.IP+"'><cn>mrhyde</cn></detected_hardware></xml>"
  util.SendLnTo(config.ServerSourceAddress, security.GosaEncrypt(dh_string, config.ModuleKey["[GOsaPackages]"]), config.Timeout)
  check(waitlong(t0, "set_activated_for_installation").XML,"<xml></xml>")

  sys, err = db.SystemGetAllDataForMAC(mac, false)
  if check(err,nil) {
    test3_result.First("cn").SetText("mrhyde")
    test3_result.First("dn").SetText("cn=mrhyde,ou=workstations,ou=systems,o=go-susi,c=de")
    test3_result.First("iphostnumber").SetText(config.IP)
    check(sys, test3_result)
  }
  
  /*
    TEST 5: Use detected_hardware to change dn
    Send detected_hardware with
      * macAddress sub-element(not attribute!) of <detected_hardware> element
      * target dn in ou=systems, with cn part cn=drjekyll
      * No cn attribute (is derivable from dn)
    Wait a bit for LDAP to be updated
    Check that object is the same as after TEST 3 except for
      * cn
      * dn
  */
  
  t0 = time.Now()
  dh_string = "<xml><header>detected_hardware</header><detected_hardware><macAddress>"+mac+"</macAddress><dn>cn=drjekyll,ou=systems,o=go-susi,c=de</dn></detected_hardware></xml>"
  util.SendLnTo(config.ServerSourceAddress, security.GosaEncrypt(dh_string, config.ModuleKey["[GOsaPackages]"]), config.Timeout)
  check(waitlong(t0, "set_activated_for_installation").XML,"<xml></xml>")

  sys, err = db.SystemGetAllDataForMAC(mac, false)
  if check(err,nil) {
    test3_result.First("cn").SetText("drjekyll")
    test3_result.First("dn").SetText("cn=drjekyll,ou=systems,o=go-susi,c=de")
    check(sys, test3_result)
  }

  /*
    TEST 6: Try to change dn to top-level (outside of base). Must fail!
    Send detected_hardware identical to the one from TEST 5, except for
      * dn: cn=drjekyll,c=de
    Check that object is unchanged and has NOT changed its DN.
    
    Delete system from LDAP
  */
  
  t0 = time.Now()
  dh_string = "<xml><header>detected_hardware</header><detected_hardware><macAddress>"+mac+"</macAddress><dn>cn=drjekyll,c=de</dn></detected_hardware></xml>"
  util.SendLnTo(config.ServerSourceAddress, security.GosaEncrypt(dh_string, config.ModuleKey["[GOsaPackages]"]), config.Timeout)
  check(waitlong(t0, "set_activated_for_installation").XML,"<xml></xml>")

  sys, err = db.SystemGetAllDataForMAC(mac, false)
  if check(err,nil) {
    check(sys, test3_result)
  }

  if sys != nil { 
    err = db.SystemReplace(hash("xml(dn(%v))",sys.Text("dn")), nil)
    if err != nil {
      fmt.Printf("ERROR! Could not delete test system. Manual cleanup of testdata/ldif may be necessary! LDAP error: %v\n", err)
    }
  }
}

func run_object_group_inheritance_tests() {
  hia := hash("xml(header(here_i_am)source(%v)target(%v)new_passwd(%v))", client_listen_address, config.ServerSourceAddress, keys[len(keys)-1])
  
  // nackt has neither gotoLdapServer nor gotoNtpServer, nor is it member of
  // an object group. 
  hia.FirstOrAdd("mac_address").SetText(db.SystemMACForName("nackt"))
  t0 := time.Now()
  send("[ClientPackages]", hia)
  msg := waitlong(t0, "new_ldap_config")
  // Since issue #129 there will always be a new_ldap_config with the server's own values if the client object has none
  //check(msg.XML, "<xml></xml>") // no new_ldap_config should be sent because nackt has no gotoLdapServer
  
  msg = waitlong(t0, "new_ntp_config")
  check(msg.XML, "<xml></xml>") // no new_ntp_config should be sent because nackt has no gotoNtpServer
  
  // ogmember1 inherits gotoLdapServer and gotoNtpServer from Objektgruppe,
  // as well as faiclass (i.e. release)
  hia.FirstOrAdd("mac_address").SetText(db.SystemMACForName("ogmember1"))
  t0 = time.Now()
  send("[ClientPackages]", hia)
  msg = waitlong(t0, "new_ldap_config")
  if check(checkTags(msg.XML,"header,new_ldap_config,source,target,admin_base?,unit_tag?,department?,ldap_base,ldap_uri+,release"), "") {
    check(msg.Key, keys[len(keys)-1])
    check(msg.IsClientMessage, true)
    check(msg.XML.Text("source"), config.ServerSourceAddress)
    check(msg.XML.Text("target"), client_listen_address)
    check(msg.XML.Text("ldap_base"), "o=go-susi,c=de")
    check(msg.XML.Get("ldap_uri"), []string{"ldap://ldap01.tvc.example.com","ldap://ldap02.tvc.example.com:389"})
    check(msg.XML.Text("release"), "plophos/4.1.0")
  }
  msg = waitlong(t0, "new_ntp_config")
  if check(checkTags(msg.XML,"header,new_ntp_config,source,target,server*"), "") {
    check(msg.XML.Get("server"), []string{"ntp01.example.com","ntp02.example.com"})
  }
  
  // ogmember2 inherits gotoLdapServer and faiclass from Objektgruppe,
  // but overrides gotoNtpServer
  hia.FirstOrAdd("mac_address").SetText(db.SystemMACForName("ogmember2"))
  t0 = time.Now()
  send("[ClientPackages]", hia)
  msg = waitlong(t0, "new_ldap_config")
  if check(checkTags(msg.XML,"header,new_ldap_config,source,target,admin_base?,unit_tag?,department?,ldap_base,ldap_uri+,release"), "") {
    check(msg.XML.Text("ldap_base"), "o=go-susi,c=de")
    check(msg.XML.Get("ldap_uri"), []string{"ldap://ldap01.tvc.example.com","ldap://ldap02.tvc.example.com:389"})
    check(msg.XML.Text("release"), "plophos/4.1.0")
  }
  msg = waitlong(t0, "new_ntp_config")
  if check(checkTags(msg.XML,"header,new_ntp_config,source,target,server*"), "") {
    check(msg.XML.Get("server"), []string{"override-ntp1.example.com","override-ntp2.example.com"})
  }
  
  // systest1 has gotoLdapServer, gotoNtpServer and faiclass
  hia.FirstOrAdd("mac_address").SetText(db.SystemMACForName("systest1"))
  t0 = time.Now()
  send("[ClientPackages]", hia)
  msg = waitlong(t0, "new_ldap_config")
  if check(checkTags(msg.XML,"header,new_ldap_config,source,target,admin_base?,unit_tag?,department?,ldap_base,ldap_uri,release"), "") {
    check(msg.Key, keys[len(keys)-1])
    check(msg.IsClientMessage, true)
    check(msg.XML.Text("source"), config.ServerSourceAddress)
    check(msg.XML.Text("target"), client_listen_address)
    check(msg.XML.Text("ldap_base"), "o=go-susi,c=de")
    check(msg.XML.Get("ldap_uri"), []string{"ldap://127.0.0.1:20088"})
    check(msg.XML.Text("release"), "plophos")
  }
  msg = waitlong(t0, "new_ntp_config")
  if check(checkTags(msg.XML,"header,new_ntp_config,source,target,server*"), "") {
    check(msg.Key, keys[len(keys)-1])
    check(msg.IsClientMessage, true)
    check(msg.XML.Text("source"), config.ServerSourceAddress)
    check(msg.XML.Text("target"), client_listen_address)
    check(msg.XML.Get("server"), []string{"cool.ntp.org"})
  }
}

func run_foreign_client_tests() {
  /*
    send new_server with 2 clients
    send confirm_new_server with 2 clients
    send 2 new_foreign_client messages
    read and check clientdb.xml for the 6 clients
  */
  
  client := []string{"1.2.3.4:5,00:00:00:00:FF:01", "11.22.33.44:55,00:00:00:00:FF:02", "111.222.33.4:555,00:00:00:00:FF:03","2.3.4.5:6,00:00:00:00:FF:04", "22.33.44.55:66,00:00:00:00:FF:05", "222.33.4.55:666,00:00:00:00:FF:06" }
  
  send("[ServerPackages]", hash("xml(header(new_server)new_server()key(%v)loaded_modules(goSusi)macaddress(00:00:00:00:00:00)client(%v)client(%v))", keys[0], client[0], client[1]))
  send("[ServerPackages]", hash("xml(header(confirm_new_server)confirm_new_server()key(%v)loaded_modules(goSusi)macaddress(00:00:00:00:00:00)client(%v)client(%v))", keys[0], client[2], client[3]))
  send("", hash("xml(header(new_foreign_client)new_foreign_client()source(%v)target(%v)client(%v)macaddress(%v))", listen_address, config.ServerSourceAddress, strings.Split(client[4],",")[0],strings.Split(client[4],",")[1]))
  send("", hash("xml(header(new_foreign_client)new_foreign_client()source(%v)target(%v)client(%v)macaddress(%v))", listen_address, config.ServerSourceAddress, strings.Split(client[5],",")[0],strings.Split(client[5],",")[1]))
  time.Sleep(reply_timeout)
  
  clientdb, err := xml.FileToHash(confdir+"/clientdb.xml")
  if check(err,nil) {
    for i := range client {
      c := clientdb.Query(xml.FilterSimple("source", listen_address, "client", strings.Split(client[i],",")[0], "macaddress", strings.Split(client[i],",")[1]))
      st := client[i] + " missing from clientdb"
      if c.First("xml") != nil { st = c.First("xml").Text("client")+","+c.First("xml").Text("macaddress") }
      check(st, client[i])
    }
  }
}

func run_here_i_am_tests() {
/*
  send here_i_am for fakeMAC1
  wait for new_foreign_client
  send here_i_am for fakeMAC2
  wait for new_foreign_client
  send here_i_am for fakeMAC1 with different address
  wait for new_foreign_client
  
  send new_server
  wait for confirm_new_server
  wait
  collect all clients from <client> elements and new_foreign_client messages
  check that there are exactly 2 clients with the correct addresses
*/

  port := []string{"7529","7535", "7574"}
  client_addr := []string{config.IP + ":" + port[0], config.IP + ":" + port[1], config.IP + ":" + port[2]}
  mac := []string{"1e:ff:c0:39:42:aa", "09:10:0d:33:ff:00", ""}
  mac[2] = mac[0] // 3rd entry is same MAC but different address
  hia := hash("xml(header(here_i_am)target(%v)new_passwd(xxx)extra_info(blafasel))", config.ServerSourceAddress)
  
  for i := 0; i < 3; i++ {
    hia.FirstOrAdd("source").SetText(client_addr[i])
    hia.FirstOrAdd("mac_address").SetText(mac[i])
    t0 := time.Now()
    send("[ClientPackages]", hia)
    nfc := wait(t0, "new_foreign_client")
    check(nfc.XML.Text("source"), config.ServerSourceAddress)
    check(nfc.XML.Text("target"), listen_address)
    check(nfc.XML.Text("client"), client_addr[i])
    check(nfc.XML.Text("macaddress"), mac[i])
    check(nfc.XML.Text("key"), "xxx")
    check(nfc.XML.Text("extra_info"), "blafasel")
  }
  
  t0 := time.Now()
  send("[ServerPackages]", hash("xml(header(new_server)new_server()key(%v)loaded_modules(goSusi)macaddress(00:00:00:00:00:00))", keys[0]))
  msg := wait(t0, "confirm_new_server")
  check(checkTags(msg.XML,"header,confirm_new_server,source,target,key,loaded_modules*,client*,macaddress"), "")
  clients := msg.XML.Get("client") 
  time.Sleep(reply_timeout) // wait for new_foreign_client messages (if any)
  for _,qe := range get(t0) {
    if qe.XML.Text("header") == "new_foreign_client" {
      clients = append(clients, fmt.Sprintf("%v,%v",qe.XML.Text("client"),qe.XML.Text("macaddress")))
    }
  }
  // Check if the 2 clients were either in cns or nfc
  // The only other client that should be in the list is the server's own internal client
  sort.Strings(clients)
  compare_clients := []string{client_addr[1]+","+mac[1], client_addr[2]+","+mac[2], config.ServerSourceAddress+","+config.MAC}
  sort.Strings(compare_clients)
  check(clients, compare_clients)
  
  // send here_i_am from MAC not in LDAP with our test client's address
  phantom_mac := "6c:d0:12:Fe:ce:50"
  hia.FirstOrAdd("source").SetText(client_listen_address)
  hia.FirstOrAdd("mac_address").SetText(phantom_mac)
  hia.FirstOrAdd("new_passwd").SetText(keys[len(keys)-1])
  t0 = time.Now()
  send("[ClientPackages]", hia)
  // check that we get a registered message
  msg = waitlong(t0, "registered")
  if check(checkTags(msg.XML,"header,registered,source,target"), "") {
    check(msg.XML.Text("source"), config.ServerSourceAddress)
    check(msg.XML.Text("target"), client_listen_address)
    check(msg.IsClientMessage, true)
    check(msg.Key, keys[len(keys)-1])
  }
  // check that we get a detect_hardware message (because there's no LDAP
  // entry for phantom_mac)
  msg = waitlong(t0, "detect_hardware")
  if check(checkTags(msg.XML,"header,detect_hardware,source,target"), "") {
    check(msg.XML.Text("source"), config.ServerSourceAddress)
    check(msg.XML.Text("target"), client_listen_address)
    check(msg.IsClientMessage, true)
    check(msg.Key, keys[len(keys)-1])
  }
  
  // change the client key, then test if the server understands messages with
  // the new key.
  send("CLIENT", hash("xml(header(new_key)new_key(new_client_key)source(%v)target(%v))", client_listen_address, config.ServerSourceAddress))
  keys[len(keys)-1] = "new_client_key"
  time.Sleep(reply_timeout)
  conn, err := net.Dial("tcp", config.ServerSourceAddress)
  check(err,nil)
  defer conn.Close()
  util.SendLn(conn, security.GosaEncrypt("<xml><header>gosa_query_jobdb</header></xml>", keys[len(keys)-1]), config.Timeout)
  reply, err := util.ReadLn(conn, config.Timeout)
  var x *xml.Hash
  if err == nil {
    reply = security.GosaDecrypt(reply, keys[len(keys)-1])
    x, err = xml.StringToHash(reply)
  }
  if check(err, nil) {
    // This test fails on gosa-si because it AFAIK it can't understand
    // GOsa messages encrypted with client keys.
    siFail(x.Text("header"),"query_jobdb")
  }
}

func run_job_processing_tests() {
  // clear database
  send("",hash("xml(header(foreign_job_updates)source(%v)target(%v)sync(all))",listen_address,config.ServerSourceAddress))
  gosa("delete_jobdb_entry", hash("xml(where())"))  
  time.Sleep(reply_timeout)
  
  var t0 time.Time

  gotoMode := func(name string) string {
    i := name[len(name)-1] - '1'
    return db.SystemGetState(Jobs[i].MAC, "gotoMode")
  }
  lock := func(name string){
    i := name[len(name)-1] - '1'
    db.SystemSetState(Jobs[i].MAC, "gotoMode", "locked")
  }
  unlock := func(name string){
    i := name[len(name)-1] - '1'
    db.SystemSetState(Jobs[i].MAC, "gotoMode", "active")
  }
  job := func(typ string, start int, name string, period int){
    i := name[len(name)-1] - '1'
    if typ == "unlock" { typ = "activate" }
    timestamp := util.MakeTimestamp(t0.Add(time.Duration(start)*time.Second))
    periodic := fmt.Sprintf("%d_seconds",period)
    if period == 0 { periodic = "none" }
    gosa("job_trigger_action_"+typ, hash("xml(target(%v)timestamp(%v)macaddress(%v)periodic(%v))",Jobs[i].MAC, timestamp, Jobs[i].MAC, periodic))
  }
  now := func(typ string, name string){
    i := name[len(name)-1] - '1'
    if typ == "unlock" { typ = "activate" }
    gosa("gosa_trigger_action_"+typ, hash("xml(target(%v)macaddress(%v))",Jobs[i].MAC, Jobs[i].MAC))
  }
  del := func(typ, name string) {
    i := name[len(name)-1] - '1'
    if typ == "unlock" { typ = "activate" }
    gosa("delete_jobdb_entry", hash("xml(where(clause(phrase(macaddress(%v))phrase(headertag(trigger_action_%v)))))",Jobs[i].MAC,typ))  
  }
  done := func(typ, name string) {
    i := name[len(name)-1] - '1'
    if typ == "unlock" { typ = "activate" }
    x := gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v))phrase(headertag(trigger_action_%v)))))",Jobs[i].MAC,typ))
    if x == nil || x.First("header") == nil { 
      util.Log(0, "ERROR! done(%v, %v)", typ, name)
      return 
    }
    x.First("header").SetText("foreign_job_updates")
    x.First("source").SetText(listen_address)
    x.First("target").SetText(config.ServerSourceAddress)
    x.First("answer1").First("status").SetText("done")
    // DO NOT x.First("answer1").First("periodic").SetText("none") because 
    // we want to test that go-susi treats this properly. An fju with status==done
    // for a job not belonging to the fju's sender can only mean that the job should
    // be removed completely, because the sender cannot know when a job is just "done"
    // and should be started again because of periodic.
    send("", x)
  }
  change_timestamp := func(typ, name string, new_start int) {
    i := name[len(name)-1] - '1'
    if typ == "unlock" { typ = "activate" }
    x := gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v))phrase(headertag(trigger_action_%v)))))",Jobs[i].MAC,typ))
    if x == nil || x.First("header") == nil { 
      util.Log(0, "ERROR! change_timestamp(%v, %v, %v)", typ, name, new_start)
      return 
    }
    x.First("header").SetText("foreign_job_updates")
    x.First("source").SetText(listen_address)
    x.First("target").SetText(config.ServerSourceAddress)
    x.First("answer1").First("timestamp").SetText(util.MakeTimestamp(t0.Add(time.Duration(new_start)*time.Second)))
    // remove periodic to simulate a message from a pre-2.7 gosa-si. This tests
    // that go-susi leaves periodic unchanged if it is missing from fju.
    x.RemoveFirst("periodic")
    send("", x)
    time.Sleep(50*time.Millisecond)
  }
  pause := func(typ, name string) {
    i := name[len(name)-1] - '1'
    if typ == "unlock" { typ = "activate" }
    x := gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v))phrase(status(waiting))phrase(headertag(trigger_action_%v)))))",Jobs[i].MAC,typ))
    if x == nil || x.First("header") == nil { 
      util.Log(0, "ERROR! pause(%v, %v)", typ, name)
      return 
    }
    x.First("header").SetText("foreign_job_updates")
    x.First("source").SetText(listen_address)
    x.First("target").SetText(config.ServerSourceAddress)
    x.First("answer1").First("status").SetText("paused")
    send("", x)
    time.Sleep(50*time.Millisecond)
  }
  unpause := func(typ, name string) {
    i := name[len(name)-1] - '1'
    if typ == "unlock" { typ = "activate" }
    x := gosa("query_jobdb", hash("xml(where(clause(phrase(macaddress(%v))phrase(status(paused))phrase(headertag(trigger_action_%v)))))",Jobs[i].MAC,typ))
    if x == nil { 
      util.Log(0, "ERROR! unpause(%v, %v): query_jobdb returned nil", typ, name)
      return 
    }
    if x.First("header") == nil || x.First("answer1") == nil { 
      util.Log(0, "ERROR! unpause(%v, %v): query_jobdb returned %v", typ, name, x)
      return 
    }
    x.First("header").SetText("foreign_job_updates")
    x.First("source").SetText(listen_address)
    x.First("target").SetText(config.ServerSourceAddress)
    x.First("answer1").First("status").SetText("waiting")
    send("", x)
    time.Sleep(50*time.Millisecond)
  }

  unlock("systest1")
  unlock("systest2")
  unlock("systest3")
  check(gotoMode("systest1"), "active")
  check(gotoMode("systest2"), "active")
  check(gotoMode("systest3"), "active")
  now("lock","systest1")
  now("lock","systest2")
  now("lock","systest3")
  time.Sleep(reply_timeout)
  check(gotoMode("systest1"), "locked")
  check(gotoMode("systest2"), "locked")
  check(gotoMode("systest3"), "locked")
  now("unlock","systest1")
  now("unlock","systest2")
  now("unlock","systest3")
  time.Sleep(reply_timeout)
  check(gotoMode("systest1"), "active")
  check(gotoMode("systest2"), "active")
  check(gotoMode("systest3"), "active")

  /*
   ATTENTION! The following tests are very timing sensitive. If you enter
   a delay in the wrong place, everything will come crashing down and you
   may even see a panic due to nil-reference in the test code.
  */

  // Because siserver timestamps have only a 1s resolution, we must make sure that
  // we start the following test on a full second. Otherwise when we later compute
  // differences to t0 and round to seconds we may be off by one which makes a lot
  // of tests fail.
  util.WaitUntil(util.ParseTimestamp(util.MakeTimestamp(time.Now().Add(2*time.Second))))

  t0 = time.Now()

  job("lock",0,   "systest1", 0)   //0: lock "systest1"
  job("lock",2,   "systest1", 0)   //2: lock "systest1"
  job("lock",4,   "systest1", 0)   //4: lock "systest1"
  job("unlock", 1,"systest1", 2)   //1, 3, 5: unlock "systest1"
  go func(){
    time.Sleep(6*time.Second)      
    del("unlock", "systest1")      //6: kill periodic unlock "systest1" preventing 7
  }()
  
  job("lock",10,  "systest2", 3)
  go func(){
    time.Sleep(100*time.Millisecond); 
    change_timestamp("lock","systest2", -1) // 0, 2, 5: lock "systest2"
    time.Sleep(6*time.Second)
    done("lock","systest2")        //6: kill periodic lock "systest2" preventing 8
  }()
  
  job("lock", 10,  "systest3", 0)
  go func(){
    time.Sleep(100*time.Millisecond); 
    pause("lock",   "systest3")
    change_timestamp("lock","systest3", 1) // 1(paused): lock "systest3"
    time.Sleep(3*time.Second)
    unpause("lock","systest3")             // 3: lock "systest3"
  }()
  job("unlock", 10, "systest3", 0)
  go func(){
    time.Sleep(100*time.Millisecond); 
    change_timestamp("unlock","systest3", 7) // 7: unlock "systest3"
  }()
  
  go func(){
    util.WaitUntil(t0.Add(1*time.Second))
    unlock("systest2")
    util.WaitUntil(t0.Add(4*time.Second))
    unlock("systest2")
    util.WaitUntil(t0.Add(6*time.Second))
    lock("systest1")
    unlock("systest2")
  }()
  
  // Complete action timeline:
  // 0: lock "systest1"
  //    lock "systest2"
  // 1: unlock "systest1"
  //    unlock "systest2" (explicit in code => no "processing"/"done" fju)
  // 2: lock "systest1"
  //    lock "systest2"
  // 3: unlock "systest1"
  //    lock "systest3"
  // 4: lock "systest1"
  //    unlock "systest2" (explicit in code => no "processing"/"done" fju)
  // 5: unlock "systest1"
  //    lock "systest2"
  // 6: lock "systest1"   (explicit in code => no "processing"/"done" fju)
  //    unlock "systest2" (explicit in code => no "processing"/"done" fju)
  // 7: unlock "systest3"
  
  
  // test that all actions happen at the correct times
  // also test that the cancelled actions do not occur at 7 and 8
  util.WaitUntil(t0.Add(500*time.Millisecond))
  check(gotoMode("systest1"), "locked")
  check(gotoMode("systest2"), "locked")
  check(gotoMode("systest3"), "active")
  util.WaitUntil(t0.Add(1500*time.Millisecond))
  check(gotoMode("systest1"), "active")
  check(gotoMode("systest2"), "active")
  check(gotoMode("systest3"), "active")
  util.WaitUntil(t0.Add(2500*time.Millisecond))
  check(gotoMode("systest1"), "locked")
  check(gotoMode("systest2"), "locked")
  check(gotoMode("systest3"), "active")
  util.WaitUntil(t0.Add(3500*time.Millisecond))
  check(gotoMode("systest1"), "active")
  check(gotoMode("systest2"), "locked")
  check(gotoMode("systest3"), "locked")
  util.WaitUntil(t0.Add(4500*time.Millisecond))
  check(gotoMode("systest1"), "locked")
  check(gotoMode("systest2"), "active")
  check(gotoMode("systest3"), "locked")
  util.WaitUntil(t0.Add(5500*time.Millisecond))
  check(gotoMode("systest1"), "active")
  check(gotoMode("systest2"), "locked")
  check(gotoMode("systest3"), "locked")
  util.WaitUntil(t0.Add(6500*time.Millisecond))
  check(gotoMode("systest1"), "locked")
  check(gotoMode("systest2"), "active")
  check(gotoMode("systest3"), "locked")
  util.WaitUntil(t0.Add(7500*time.Millisecond))
  check(gotoMode("systest1"), "locked")
  check(gotoMode("systest2"), "active")
  check(gotoMode("systest3"), "active")
  util.WaitUntil(t0.Add(8500*time.Millisecond))
  check(gotoMode("systest1"), "locked")
  check(gotoMode("systest2"), "active")
  check(gotoMode("systest3"), "active")
  
  unlock("systest1")
  
  // fju timeline (excluding "processing" and "done" which occur for each
  // of the actions in the timeline above):
  // 0: lock   "systest1" 0,0  waiting
  //    lock   "systest1" 2,0  waiting
  //    lock   "systest1" 4,0  waiting
  //    unlock "systest1" 1,2  waiting
  //    lock   "systest2" 10,3 waiting
  //    lock   "systest2" -1,3 waiting
  //    lock   "systest3" 10,0 waiting
  //    lock   "systest3" 10,0 paused
  //    lock   "systest3" 1,0  paused
  //    unlock "systest3" 10,0 waiting
  //    unlock "systest3" 7,0  waiting
  //    lock   "systest2" 2,3  waiting
  // 1: unlock "systest1" 3,2  waiting
  // 2: lock   "systest2" 5,3  waiting
  // 3: lock   "systest3" 1,0  waiting
  //    unlock "systest1" 5,2  waiting
  // 4: ----
  // 5: unlock "systest1" 7,2  waiting
  //    lock   "systest2" 8,3  waiting
  // 6: unlock "systest1" 7,0(2)  done
  //    lock   "systest2" 8,0(3)  done

  quantize := func(t time.Time) int {
    dur := t.Sub(t0)
    if dur < 0 { return int((dur-500*time.Millisecond)/time.Second) }
    return int((dur+500*time.Millisecond)/time.Second)
  }
  
  // collect all fjus in format (6 unlock systest1 7,2  done)
  var messages []*queueElement = get(t0)
  msgset := map[string]string{}
  for _, msg := range messages {
    if msg.XML.Text("header") != "foreign_job_updates" { continue }
    for _, tag := range msg.XML.Subtags() {
      if !strings.HasPrefix(tag, "answer") { continue }
      for job := msg.XML.First(tag); job != nil; job = job.Next() {
        typ := job.Text("headertag")
        if strings.HasPrefix(typ, "trigger_action_") { typ = typ[15:] }
        if typ == "activate" { typ = "unlock" }
        
        peri := job.Text("periodic")
        if peri == "none" || peri == "" { 
          peri = "0" 
        } else {
          peri = peri[0:strings.Index(peri,"_")]
        }
        
        name := db.SystemPlainnameForMAC(job.Text("macaddress"))
        ts := quantize(util.ParseTimestamp(job.Text("timestamp")))
        //fmt.Printf("%v: %v => %v => %v\n",name,job.Text("timestamp"),util.ParseTimestamp(job.Text("timestamp")),ts)
        msgset[fmt.Sprintf("(%v %v %v %v,%v %v)",quantize(msg.Time),typ,name,ts,peri,job.Text("status"))] = ""
      }
    }
  }
  
  fju := func(when int, typ, name string, ts,peri int, status string) string {
    s := fmt.Sprintf("(%v %v %v %v,%v %v)",when,typ,name,ts,peri,status)
    if _, ok := msgset[s]; ok { delete(msgset,s); return "" }
    return "Missing fju: "+s
  }
  
  // now check that all fjus we expect are present
  check(fju(0,"lock",   "systest1", 0,0, "waiting"),"")
  check(fju(0,"lock",   "systest1", 2,0, "waiting"),"")
  check(fju(0,"lock",   "systest1", 4,0,  "waiting"),"")
  check(fju(0,"unlock", "systest1", 1,2,  "waiting"),"")
  check(fju(0,"lock",   "systest2", 10,3, "waiting"),"")
  check(fju(0,"lock",   "systest2", -1,3, "waiting"),"")
  check(fju(0,"lock",   "systest3", 10,0, "waiting"),"")
  check(fju(0,"lock",   "systest3", 10,0, "paused"),"")
  check(fju(0,"lock",   "systest3", 1,0,  "paused"),"")
  check(fju(0,"unlock", "systest3", 10,0, "waiting"),"")
  check(fju(0,"unlock", "systest3", 7,0,  "waiting"),"")
  check(fju(0,"lock",   "systest2", 2,3,  "waiting"),"")
  check(fju(1,"unlock", "systest1", 3,2,  "waiting"),"")
  check(fju(2,"lock",   "systest2", 5,3,  "waiting"),"")
  check(fju(3,"lock",   "systest3", 1,0,  "waiting"),"")
  check(fju(3,"unlock", "systest1", 5,2,  "waiting"),"")
  check(fju(5,"unlock", "systest1", 7,2,  "waiting"),"")
  check(fju(5,"lock",   "systest2", 8,3,  "waiting"),"")
  check(fju(6,"unlock", "systest1", 7,0,  "done"),"")
  check(fju(6,"lock",   "systest2", 8,0,  "done"),"")
  
  check(fju(0,"lock",   "systest1", 0,0, "done"),"")
  check(fju(0,"lock",   "systest2",-1,3, "done"),"")
  check(fju(1,"unlock", "systest1", 1,2, "done"),"")
  check(fju(2,"lock",   "systest1", 2,0, "done"),"")
  check(fju(2,"lock",   "systest2", 2,3, "done"),"")
  check(fju(3,"unlock", "systest1", 3,2, "done"),"")
  check(fju(3,"lock",   "systest3", 1,0, "done"),"")
  check(fju(4,"lock",   "systest1", 4,0, "done"),"")
  check(fju(5,"unlock", "systest1", 5,2, "done"),"")
  check(fju(5,"lock",   "systest2", 5,3, "done"),"")
  check(fju(7,"unlock", "systest3", 7,0, "done"),"")
  
  check(fju(0,"lock",   "systest1", 0,0, "processing"),"")
  check(fju(0,"lock",   "systest2",-1,3, "processing"),"")
  check(fju(1,"unlock", "systest1", 1,2, "processing"),"")
  check(fju(2,"lock",   "systest1", 2,0, "processing"),"")
  check(fju(2,"lock",   "systest2", 2,3, "processing"),"")
  check(fju(3,"unlock", "systest1", 3,2, "processing"),"")
  check(fju(3,"lock",   "systest3", 1,0, "processing"),"")
  check(fju(4,"lock",   "systest1", 4,0, "processing"),"")
  check(fju(5,"unlock", "systest1", 5,2, "processing"),"")
  check(fju(5,"lock",   "systest2", 5,3, "processing"),"")
  check(fju(7,"unlock", "systest3", 7,0, "processing"),"")
  
  check(msgset, map[string]string{})
  
  // IDEA for further testing:
  // now test that the testee doesn't execute jobs from other siservers,
  // even when 
  // * their timestamp is changed via gosa_update_status_jobdb_entry
  // * their timestamp is changed via fju
  // * their status is set to "processing" via gosa_update_status_jobdb_entry
  // * their status is set to "processing" via fju
  
}

func run_startup_tests() {
  if launched_daemon {
    check_new_server_on_startup(Jobs[0])
  } else {
    // We need this in the database for the later test whether go-susi reacts
    // to new_server by sending its jobdb. This same call is contained in
    // check_new_server_on_startup()
    trigger_first_test_job(Jobs[0])
  }
  
  // Send new_server and check that we receive confirm_new_server in response
  t0 := time.Now()
  keys[0] = "new_server_key"
  send("[ServerPackages]", hash("xml(header(new_server)new_server()key(%v)loaded_modules(goSusi)macaddress(00:00:00:00:00:00))", keys[0]))
  msg := wait(t0, "confirm_new_server")
  check(checkTags(msg.XML,"header,confirm_new_server,source,target,key,loaded_modules*,client*,macaddress"), "")
  check(msg.Key, config.ModuleKey["[ServerPackages]"])
  check(strings.Split(msg.XML.Text("source"),":")[0], msg.SenderIP)
  check(msg.XML.Text("source"), config.ServerSourceAddress)
  check(msg.XML.Text("target"), listen_address)
  check(msg.XML.Text("key"), "new_server_key")
  check(len(msg.XML.Get("confirm_new_server"))==1 && msg.XML.Text("confirm_new_server")=="", true)
  siFail(strings.Contains(msg.XML.Text("loaded_modules"), "goSusi"), true)
  check(macAddressRegexp.MatchString(msg.XML.Text("macaddress")), true)
  clientsOk := true
  for _, client := range msg.XML.Get("client") {
    if !clientRegexp.MatchString(client) { clientsOk = false }
  }
  check(clientsOk, true)
  
  // go-susi also sends foreign_job_updates in response to new_server
  msg = wait(t0, "foreign_job_updates")
  if siFail(checkTags(msg.XML, "header,source,target,answer1,sync?"), "") {
    check_foreign_job_updates(msg, "new_server_key", config.ServerSourceAddress, Jobs[0].Plainname, Jobs[0].Periodic, "waiting", "none", Jobs[0].MAC, Jobs[0].Trigger(), Jobs[0].Timestamp)
  }

  t0 = time.Now()
  x := gosa(Jobs[1].Type, hash("xml(target(%v)timestamp(%v)macaddress(%v)periodic(%v))",Jobs[1].MAC, Jobs[1].Timestamp, Jobs[1].MAC, Jobs[1].Periodic))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  check(x.Text("header"), "answer")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  check(x.Text("answer1"), "0")
  
  msg = wait(t0, "foreign_job_updates")
  check_foreign_job_updates(msg, "new_server_key", config.ServerSourceAddress,  Jobs[1].Plainname, Jobs[1].Periodic, "waiting", "none",Jobs[1].MAC, Jobs[1].Trigger(), Jobs[1].Timestamp)
}

func run_foreign_job_updates_tests() {
  // clear jobdb  
  x := gosa("delete_jobdb_entry", hash("xml(where())"))
  
  // Because the above delete may affect jobs belonging to the test server,
  // go-susi may not delete all jobs directly and instead forward the request to the
  // test server. Wait a little to make sure the communication is finished.
  time.Sleep(reply_timeout)
  
  // try to add jobs with incorrect or missing <source> and/or incorrect or missing <siserver>
  // All of these should be rejected by the server, except for the 1 correct job with
  // listen_address/"localhost"
  count := 10
  for _, source := range []string{listen_address, "foo","1.2.3.4","1.2.3:9999","","missing"} {
    x := hash("xml(header(foreign_job_updates)source(%v)target(%v))",source,config.ServerSourceAddress)
    if source == "missing" { x.RemoveFirst("source") }
    i := 1
    for _, siserver := range []string{"localhost","foo","1.2.3.4","1.2.3:9999","","missing" } {
      job := hash("answer%d(plainname(%v)progress(none)status(waiting)siserver(%v)modified(1)macaddress(00:00:00:00:00:%d)targettag(00:00:00:00:00:%d)timestamp(%v)id(%d)headertag(%v)result(none))",
                   i, Jobs[0].Plainname, siserver, count, count, Jobs[0].Timestamp, count, Jobs[0].Trigger())
      count++
      i++
      if siserver == "missing" { job.Remove(xml.FilterSimple("siserver")) }
      job.FirstOrAdd("xmlmessage").SetText(base64.StdEncoding.EncodeToString([]byte(hash("xml(header(%v)source(%v)target(%v)timestamp(%v)macaddress(%v))",Jobs[0].Type,"GOSA",job.Text("macaddress"),Jobs[0].Timestamp,job.Text("macaddress")).String())))
      x.AddClone(job)
      util.SendLnTo(config.ServerSourceAddress, security.GosaEncrypt(x.String(), keys[0]), config.Timeout)
    }  
  }
  
  // Wait for messages to be processed, because send() doesn't wait.
  time.Sleep(reply_timeout)
  
  // Check the jobdb to verify that only the one correct job was added to the database
  x = gosa("query_jobdb", hash("xml(where())"))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  job := x.First("answer1")
  if job != nil {
     // NOTE: go-susi converts "localhost" => source
    check_answer(job, Jobs[0].Plainname, "none", "waiting", listen_address, "00:00:00:00:00:10", Jobs[0].Timestamp, "none", Jobs[0].Trigger())
  }

  // Send empty foreign_job_updates with sync "ordered" and test that this does NOT clear out the above job
  x = hash("xml(header(foreign_job_updates)source(%v)target(%v)sync(ordered))",listen_address,config.ServerSourceAddress)
  send("",x)
  
  // Wait for messages to be processed, because send() doesn't wait.
  time.Sleep(reply_timeout)
  
  // Check the jobdb to verify that the job's still there
  x = gosa("query_jobdb", hash("xml(where())"))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")

  // Send empty foreign_job_updates with sync all and test that this clears out the above job
  x = hash("xml(header(foreign_job_updates)source(%v)target(%v)sync(all))",listen_address,config.ServerSourceAddress)
  send("",x)
  
  // Wait for messages to be processed, because send() doesn't wait.
  time.Sleep(reply_timeout)
  
  // Check the jobdb to verify that it is now empty.
  x = gosa("query_jobdb", hash("xml(where())"))
  check(checkTags(x, "header,source,target,session_id?"),"")
  
  // Now we test that when the testee forwards deletions it includes the 
  // original id (rather than its own)
  job = Jobs[0].Hash()
  job.First("id").SetText("0815")
  fju := xml.NewHash("xml", "header", "foreign_job_updates")
  fju.Add("source", listen_address)
  fju.AddClone(job)
  send("", fju)
  time.Sleep(reply_timeout)
  x = gosa("query_jobdb", hash("xml(where())"))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  t0 := time.Now()
  gosa("delete_jobdb_entry", hash("xml(where())"))
  msg := wait(t0, "foreign_job_updates")
  if check(checkTags(msg.XML, "header,source,target,answer1,sync?"), "") {
    check_foreign_job_updates(msg, keys[0], listen_address, Jobs[0].Plainname, "", "done", "none", Jobs[0].MAC, Jobs[0].Trigger(), Jobs[0].Timestamp)
    siFail(msg.XML.First("answer1").Text("id"), "0815")
  }
  
  // Clear out the above test job. For gosa-si, the gosa_delete_jobdb above has
  // already done that but go-susi waits for our fju.
  fju.First("answer1").First("status").SetText("done")
  t0 = time.Now()
  send("", fju)
  // check that we receive NO fju
  msg = wait(t0, "foreign_job_updates")
  check(msg.XML.Text("header"), "")
  time.Sleep(1*time.Second)
  // Check the jobdb to verify that it is now empty.
  x = gosa("query_jobdb", hash("xml(where())"))
  check(checkTags(x, "header,source,target,session_id?"),"")
  
  // Now we test that when the testee forwards a modification it includes
  // the original id (rather than its own)
  job = Jobs[0].Hash()
  job.First("id").SetText("textID")
  fju = xml.NewHash("xml", "header", "foreign_job_updates")
  fju.Add("source", listen_address)
  fju.AddClone(job)
  send("", fju)
  time.Sleep(reply_timeout)
  x = gosa("query_jobdb", hash("xml(where())"))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  x.RemoveFirst("session_id")
  x.First("header").SetText("foreign_job_updates")
  x.First("target").SetText(listen_address)
  x.Add("sync", "ordered")
  msg = &queueElement{XML:x,Key:keys[0]}
  check_foreign_job_updates(msg, keys[0], listen_address, Jobs[0].Plainname, Jobs[0].Periodic, "waiting", "none", Jobs[0].MAC, Jobs[0].Trigger(), Jobs[0].Timestamp)
  t0 = time.Now()
  gosa("update_status_jobdb_entry", hash("xml(where()update(progress(20)))"))
  msg = wait(t0, "foreign_job_updates")
  
  if check(checkTags(msg.XML, "header,source,target,answer1,sync?"), "") {
    check_foreign_job_updates(msg, keys[0], listen_address, Jobs[0].Plainname, Jobs[0].Periodic, "waiting", "20", Jobs[0].MAC, Jobs[0].Trigger(), Jobs[0].Timestamp)
    check(msg.XML.First("answer1").Text("id"), "textID")
  }
  
  // Clear out the test job. 
  send("",hash("xml(header(foreign_job_updates)source(%v)target(%v)sync(all))",listen_address,config.ServerSourceAddress))
  gosa_noreply("delete_jobdb_entry", hash("xml(where())"))
  
  // Wait for db to be cleared before creating new jobs to prevent
  // the delete command from being processed after the job creations due
  // to goroutine scheduling
  time.Sleep(config.GosaQueryJobdbMaxDelay)
  
  // create a job on the testee
  gosa_noreply(Jobs[0].Type, hash("xml(target(%v)timestamp(%v)macaddress(%v))",Jobs[0].MAC,Jobs[0].Timestamp,Jobs[0].MAC))
  time.Sleep(config.GosaQueryJobdbMaxDelay)
  // check if it's been successfully created
  x = gosa("query_jobdb", hash("xml(where())"))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  job = x.First("answer1")
  check_answer(job, Jobs[0].Plainname, "none", "waiting", config.ServerSourceAddress, Jobs[0].MAC, Jobs[0].Timestamp, "none", Jobs[0].Trigger())
  
  // send fju with wrong id. Because our test server has identified itself as
  // goSusi the testee should ignore the fju because goSusi protocol requires
  // sending the correct id.
  id := job.Text("id")
  job.First("id").SetText("not_the_actual_id")
  job.First("status").SetText("done")
  fju = xml.NewHash("xml", "header", "foreign_job_updates")
  fju.Add("source", listen_address)
  fju.AddClone(job)
  t0 = time.Now()
  send("", fju)
  // check that we receive NO fju
  msg = wait(t0, "foreign_job_updates")
  check(msg.XML.Text("header"), "")
  // check that the job's still there
  x = gosa("query_jobdb", hash("xml(where())"))
  if siFail(checkTags(x, "header,source,target,answer1,session_id?"),"") {
    job = x.First("answer1")
    check_answer(job, Jobs[0].Plainname, "none", "waiting", config.ServerSourceAddress, Jobs[0].MAC, Jobs[0].Timestamp, "none", Jobs[0].Trigger())  
  }
  
  // now send the fju with the correct id and check it causes the job to be removed.
  // This is Case 1.1 in foreign_job_updates.go
  job.First("id").SetText(id)
  job.First("status").SetText("done")
  fju = xml.NewHash("xml", "header", "foreign_job_updates")
  fju.Add("source", listen_address)
  fju.AddClone(job)
  t0 = time.Now()
  send("", fju)
  // check that we receive fju (this fails on gosa-si because gosa-si does not
  // send fju for changes received via fju)
  msg = wait(t0, "foreign_job_updates")
  if siFail(checkTags(msg.XML, "header,source,target,answer1,sync?"), "") {
    check_foreign_job_updates(msg, keys[0], config.ServerSourceAddress, Jobs[0].Plainname, "", "done", "none", Jobs[0].MAC, Jobs[0].Trigger(), Jobs[0].Timestamp)
  }
  
  // check that the job's gone
  x = gosa("query_jobdb", hash("xml(where())"))
  check(checkTags(x, "header,source,target,session_id?"),"")
  
  // Send new_server without "goSusi" to make testee treat us as gosa-si
  t0 = time.Now()
  send("[ServerPackages]", hash("xml(header(new_server)new_server()key(%v)loaded_modules(foo)macaddress(00:00:00:00:00:00))", keys[0]))
  msg = wait(t0, "confirm_new_server")
  
  // create a job on the testee
  gosa(Jobs[0].Type, hash("xml(target(%v)timestamp(%v)macaddress(%v))",Jobs[0].MAC,Jobs[0].Timestamp,Jobs[0].MAC))
  time.Sleep(reply_timeout)
  // check if it's been successfully created
  x = gosa("query_jobdb", hash("xml(where())"))
  if check(checkTags(x, "header,source,target,answer1,session_id?"),"") {
    job = x.First("answer1")
    check_answer(job, Jobs[0].Plainname, "none", "waiting", config.ServerSourceAddress, Jobs[0].MAC, Jobs[0].Timestamp, "none", Jobs[0].Trigger())  
  }
  
  // check that the job is removed even when we send the wrong id
  // This is Case 1.2 in foreign_job_updates.go
  job.First("id").SetText("not_the_actual_id")
  job.First("status").SetText("done")
  fju = xml.NewHash("xml", "header", "foreign_job_updates")
  fju.Add("source", listen_address)
  fju.AddClone(job)
  t0 = time.Now()
  send("", fju)
  // check that we receive fju
  msg = wait(t0, "foreign_job_updates")
  siFail(msg.XML.Text("header"), "foreign_job_updates")
  // Check the job's actually gone
  x = gosa("query_jobdb", hash("xml(where())"))
  check(checkTags(x, "header,source,target,session_id?"),"")
  
  // Now identify our test server as goSusi again
  send("[ServerPackages]", hash("xml(header(new_server)new_server()key(%v)loaded_modules(goSusi)macaddress(00:00:00:00:00:00))", keys[0]))
  time.Sleep(4*time.Second) // make sure all full sync fjus are gone
  
  // Case 2 in foreign_job_updates.go
  // Updated job belongs to the sender (i.e. our test server)
  job.First("id").SetText("my_id")
  job.First("siserver").SetText(listen_address)
  job.First("status").SetText("waiting")
  fju = xml.NewHash("xml", "header", "foreign_job_updates")
  fju.Add("source", listen_address)
  fju.AddClone(job)
  time.Sleep(2*time.Second)
  t0 = time.Now()
  send("", fju)
  time.Sleep(2*time.Second)
  // check that we receive NO fju
  msg = wait(t0, "foreign_job_updates")
  check(msg.XML.Text("header"), "")
  // check that the job is there
  x = gosa("query_jobdb", hash("xml(where())"))
  if check(checkTags(x, "header,source,target,answer1,session_id?"),"") {
    job = x.First("answer1")
    check_answer(job, Jobs[0].Plainname, "none", "waiting", listen_address, Jobs[0].MAC, Jobs[0].Timestamp, "none", Jobs[0].Trigger())  
  }
  // modify the job
  fju.First("answer1").First("progress").SetText("99")
  fju.First("answer1").First("status").SetText("paused")
  t0 = time.Now()
  send("", fju)
  // check that we receive NO fju
  msg = wait(t0, "foreign_job_updates")
  check(msg.XML.Text("header"), "")
  // check that the job is updated
  x = gosa("query_jobdb", hash("xml(where())"))
  if check(checkTags(x, "header,source,target,answer1,session_id?"),"") {
    job = x.First("answer1")
    check_answer(job, Jobs[0].Plainname, "99", "paused", listen_address, Jobs[0].MAC, Jobs[0].Timestamp, "none", Jobs[0].Trigger())  
  }
  
  // Now delete the job, using an incorrect headertag. This fails on gosa-si but
  // succeeds on go-susi because go-susi used the id to select the job
  fju.First("answer1").First("headertag").SetText("incorrect")
  fju.First("answer1").First("status").SetText("done")
  send("", fju)
  time.Sleep(reply_timeout)
  // check that the job is gone
  x = gosa("query_jobdb", hash("xml(where())"))
  siFail(checkTags(x, "header,source,target,session_id?"),"")
}

func run_postprocess_logfiles_tests() {
  t0 := time.Now().Unix()  

  // default case 1
  logfiledir := "testdata/var_log_fai/01:02:03:04:05:06/install_20151116_191910"
  macaddress := "01:02:03:04:05:06"
  message.Postprocess_logfiles(logfiledir, macaddress)
  lastrunTime, _ := strconv.ParseInt(db.SystemGetState(macaddress, "FAIlastrunTime"), 10, 64)
  check(db.SystemGetState(macaddress, "faiState"), "localboot")
  check(db.SystemGetState(macaddress, "FAIlastrunClass"), "Modul_Standard _ADD_SOMETHING_SPECIAL :wanderer")
  check(lastrunTime >= t0, true)
  
  // default case 2 (read from ldap2fai.log as fallback/workaround)
  logfiledir = "testdata/var_log_fai/01:02:03:04:05:06/softupdate_20160108_150408"
  macaddress = "01:02:03:04:05:06"
  message.Postprocess_logfiles(logfiledir, macaddress)
  lastrunTime, _ = strconv.ParseInt(db.SystemGetState(macaddress, "FAIlastrunTime"), 10, 64)
  check(db.SystemGetState(macaddress, "faiState"), "localboot")
  check(db.SystemGetState(macaddress, "FAIlastrunClass"), "Modul_Standard _ADD_SOMETHING_SPECIAL :wanderer")
  check(lastrunTime >= t0, true)
  
  // default case 3 (more complicated release-string "tramp/5.0.0something")
  logfiledir = "testdata/var_log_fai/01:02:03:04:05:06/install_20151116_231910"
  macaddress = "01:02:03:04:05:06"
  message.Postprocess_logfiles(logfiledir, macaddress)
  lastrunTime, _ = strconv.ParseInt(db.SystemGetState(macaddress, "FAIlastrunTime"), 10, 64)
  check(db.SystemGetState(macaddress, "faiState"), "localboot")
  check(db.SystemGetState(macaddress, "FAIlastrunClass"), "Modul_Standard _ADD_SOMETHING_SPECIAL :tramp/5.0.0something")
  check(lastrunTime >= t0, true)
  
  // default case 4 (more complicated release-string "tramp/5.0.0something" + fallback/workaround)
  logfiledir = "testdata/var_log_fai/01:02:03:04:05:06/softupdate_20160108_230408"
  macaddress = "01:02:03:04:05:06"
  message.Postprocess_logfiles(logfiledir, macaddress)
  lastrunTime, _ = strconv.ParseInt(db.SystemGetState(macaddress, "FAIlastrunTime"), 10, 64)
  check(db.SystemGetState(macaddress, "faiState"), "localboot")
  check(db.SystemGetState(macaddress, "FAIlastrunClass"), "Modul_Standard _ADD_SOMETHING_SPECIAL :tramp/5.0.0something")
  check(lastrunTime >= t0, true)
  
  // now with invalid logfiledir
  logfiledir = "testdata/var_log_fai/01:02:03:04:05:06/somethingNonExistent"
  macaddress = "01:02:03:04:05:06"
  message.Postprocess_logfiles(logfiledir, macaddress)
  lastrunTime, _ = strconv.ParseInt(db.SystemGetState(macaddress, "FAIlastrunTime"), 10, 64)
  check(db.SystemGetState(macaddress, "faiState"), "localboot")
  check(db.SystemGetState(macaddress, "FAIlastrunClass"), "")
  check(lastrunTime >= t0, true)

  // now with invalid macaddress
  logfiledir = "testdata/var_log_fai/01:02:03:04:05:06/install_20151116_191910"
  macaddress = "01:42:42:42:42:06"
  message.Postprocess_logfiles(logfiledir, macaddress)
  check(db.SystemGetState(macaddress, "faiState"), "")
  check(db.SystemGetState(macaddress, "FAIlastrunClass"), "")
  check(db.SystemGetState(macaddress, "FAIlastrunTime"), "")
}

func check_multiple_requests_over_one_connection() {
  get_all_jobs := hash("xml(header(gosa_query_jobdb)source(GOSA)target(GOSA)where())")
  
  conn, err := net.Dial("tcp", config.ServerSourceAddress)
  check(err, nil)
  if err != nil { return }
  defer conn.Close()
  
  for i :=0 ; i < 3; i++ {
    util.SendLn(conn, "\n\n\r\r\r\n\r\r\n", config.Timeout) // test that empty lines don't hurt
    util.SendLn(conn, security.GosaEncrypt(get_all_jobs.String(), config.ModuleKey["[GOsaPackages]"]), config.Timeout)
    var x *xml.Hash
    reply, err := util.ReadLn(conn, config.Timeout)
    if err == nil {
      reply = security.GosaDecrypt(reply, config.ModuleKey["[GOsaPackages]"])
      x, err = xml.StringToHash(reply)
    }
    check(err, nil)
    check(checkTags(x,"header,source,target,session_id?,answer1,answer2"), "")
  }
}


// Check that go-susi forcibly closes the connection if it encounters an
// unknown <header>.
func check_connection_drop_on_error1() {
  x := hash("xml(header(gibberish)source(GOSA)target(GOSA))")
  
  conn, err := net.Dial("tcp", config.ServerSourceAddress)
  check(err, nil)
  if err != nil { return }
  defer conn.Close()
  
  util.SendLn(conn, security.GosaEncrypt(x.String(), config.ModuleKey["[GOsaPackages]"]), config.Timeout)
  reply, err := util.ReadLn(conn, config.Timeout)
  if err == nil {
    reply = security.GosaDecrypt(reply, config.ModuleKey["[GOsaPackages]"])
    x, err = xml.StringToHash(reply)
  }
  check(err, nil)
  
  check(len(x.Text("error_string")) > 0, true)
  
  // Server should drop connection immediately after sending error reply.
  // Give it just a little bit of time.
  time.Sleep(1 * time.Second)
  
  t0 := time.Now()
  conn.SetDeadline(time.Now().Add(5*time.Second))
  _, err = conn.Read(make([]byte, 1)) // should terminate with error immediately
  check(err, io.EOF)
  check(time.Since(t0) < 1 * time.Second, true)
}

// Check that go-susi forcibly closes the connection if it encounters an 
// undecryptable message.
func check_connection_drop_on_error2() {
  x := hash("xml(header(gibberish)source(GOSA)target(GOSA))")
  
  conn, err := net.Dial("tcp", config.ServerSourceAddress)
  check(err, nil)
  if err != nil { return }
  defer conn.Close()
  
  util.SendLn(conn, security.GosaEncrypt(x.String(), "wuseldusel"), config.Timeout)
  reply,_ := util.ReadLn(conn, config.Timeout)
  x, err = xml.StringToHash(reply)
  check(err, nil)
  
  check(len(x.Text("error_string")) > 0, true)
  
  // Server should drop connection immediately after sending error reply.
  // Give it just a little bit of time.
  time.Sleep(1 * time.Second)
  
  t0 := time.Now()
  conn.SetDeadline(time.Now().Add(5*time.Second))
  _, err = conn.Read(make([]byte, 1)) // should terminate with error immediately
  check(err, io.EOF)
  check(time.Since(t0) < 1 * time.Second, true)
}

// Checks that on startup go-susi sends new_server to the test server listed
// in [ServerPackages]/address
func check_new_server_on_startup(job Job) {
  test_mac := job.MAC
  test_name:= job.Plainname
  test_timestamp:=job.Timestamp
  // Test if daemon sends us new_server upon startup
  msg := wait(StartTime, "new_server")
  
  // gosa-si ignored our address= entry because it thinks it refers to
  // itself because GosaSupportDaemon.pm:is_local does not consider the port
  siFail(len(msg.XML.Subtags()) > 0, true)
  if len(msg.XML.Subtags()) > 0 {

    // Verify that new_server message is according to spec
    check(checkTags(msg.XML,"header,new_server,source,target,key,loaded_modules*,client*,macaddress"), "")
    check(msg.Key, config.ModuleKey["[ServerPackages]"])

    siFail(strings.Split(msg.XML.Text("source"),":")[0], msg.SenderIP)
    siFail(msg.XML.Text("source"), config.ServerSourceAddress)
    siFail(msg.XML.Text("target"), listen_address)

    check(len(msg.XML.Get("new_server"))==1 && msg.XML.Text("new_server")=="", true)
    siFail(strings.Contains(msg.XML.Text("loaded_modules"), "goSusi"), true)

    check(macAddressRegexp.MatchString(msg.XML.Text("macaddress")), true)
    clientsOk := true
    for _, client := range msg.XML.Get("client") {
      if !clientRegexp.MatchString(client) { clientsOk = false }
    }
    check(clientsOk, true)

    // send confirm_new_server with a different key to check that c_n_s does not
    // need to use the same key as new_server
    // We need to wait a little first, because due to the complications of starting
    // both go-susi and our test server go-susi sometimes sees the test server as
    // down for a moment which causes 2 new_server messages being sent, the second
    // of which (if it is received by the test server after we send our
    // confirm_new_server) may overwrite keys[0].
    time.Sleep(3*time.Second)
    keys[0] = "confirm_new_server_key"
    send("[ServerPackages]", hash("xml(header(confirm_new_server)confirm_new_server()key(%v)loaded_modules(goSusi)macaddress(01:02:03:04:05:06))",keys[0]))

    // Wait a little to make sure the server has processed our confirm_new_server
    // and activated our provided key
    time.Sleep(reply_timeout)

    // send job_trigger_action to check that we get a foreign_job_updates encrypted 
    // with the key we set via confirm_new_server above
    t0 := time.Now()
    
    trigger_first_test_job(Jobs[0])
    
    msg = wait(t0, "foreign_job_updates")
    check_foreign_job_updates(msg, "confirm_new_server_key", config.ServerSourceAddress, test_name, "7_days", "waiting", "none", test_mac, "trigger_action_wake", test_timestamp)
  }
}

func trigger_first_test_job(job Job) {
  typ:=job.Type
  test_mac:=job.MAC
  test_timestamp:=job.Timestamp
  x := gosa(typ, hash("xml(target(%v)timestamp(%v)macaddress(%v)periodic(7_days))",test_mac, test_timestamp, test_mac))
  check(checkTags(x, "header,source,target,answer1,session_id?"),"")
  check(x.Text("header"), "answer")
  siFail(x.Text("source"), config.ServerSourceAddress)
  check(x.Text("target"), "GOSA")
  check(x.Text("answer1"), "0")
}

func check_foreign_job_updates(msg *queueElement, test_key, test_server, test_name, test_periodic, test_status, test_progress, test_mac, action, test_timestamp string) {
  _, file, line, _ := runtime.Caller(1)
  file = file[strings.LastIndex(file, "/")+1:]
  fmt.Printf("== check_foreign_job_updates sub-tests (%v:%v) ==\n", file, line)
      
  check(checkTags(msg.XML, "header,source,target,answer1,sync?"), "")
  check(msg.Key, test_key)
  check(msg.XML.Text("header"), "foreign_job_updates")
  siFail(msg.XML.Text("source"), config.ServerSourceAddress)
  siFail(msg.XML.Text("target"), listen_address)
  siFail(msg.XML.Text("sync") == "ordered" || msg.XML.Text("sync") == "all", true)
  job := msg.XML.First("answer1")
  check(job != nil, true)
  if job != nil {
    check(checkTags(job, "plainname,periodic?,progress,status,siserver,modified,targettag,macaddress,timestamp,id,original_id?,headertag,result,xmlmessage"),"")
    
    // plainname is optional but when it is supplied, make sure it's correct
    plainname := job.Text("plainname")
    if plainname != "" && plainname != "none" {
      check(job.Text("plainname"), test_name)
    }
    
    peri := job.Text("periodic")
    if peri == "none" { peri = "" }
    siFail(peri, test_periodic)
    check(job.Text("progress"), test_progress)
    check(job.Text("status"), test_status)
    siFail(job.Text("siserver"), test_server)
    check(job.Text("targettag"), test_mac)
    check(job.Text("macaddress"), test_mac)
    check(job.Text("timestamp"), test_timestamp)
    check(job.Text("headertag"), action)
    check(job.Text("result"), "none")
    
    // The strange Join/Fields combo gets rid of the whitespace which gosa-si introduces into xmlmessage
    xmlmessage_txt := strings.Join(strings.Fields(job.Text("xmlmessage")),"")
    siFail(xmlmessage_txt, job.Text("xmlmessage"))
    decoded, _ := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(job.Text("xmlmessage")),""))
    xmlmessage, err := xml.StringToHash(string(decoded))
    check(err, nil)
    check(checkTags(xmlmessage, "header,source,target,timestamp,periodic?,macaddress"), "")
    check(xmlmessage.Text("header"), "job_" + action)
    check(xmlmessage.Text("source"), "GOSA")
    check(xmlmessage.Text("target"), test_mac)
    check(xmlmessage.Text("timestamp"), test_timestamp)
    if test_periodic != "" { // do not test if periodic="" because this may be due to a delete in which case the xmlmessage doesn't match
      check(xmlmessage.Text("periodic"), test_periodic)
    }
    check(xmlmessage.Text("macaddress"), test_mac)
  }
}

func check_answer(a *xml.Hash, name, progress, status, siserver, mac, timestamp, periodic, headertag string) {
  _, file, line, _ := runtime.Caller(1)
  file = file[strings.LastIndex(file, "/")+1:]
  fmt.Printf("== check_answer sub-tests (%v:%v) ==\n", file, line)
      
  check(checkTags(a, "plainname,periodic?,progress,status,siserver,modified,targettag,macaddress,timestamp,id,original_id?,headertag,result,xmlmessage"),"")
  check(a.Text("plainname"), name)
  check(a.Text("progress"), progress)
  check(a.Text("status"), status)
  siFail(a.Text("siserver"), siserver)
  check(a.Text("targettag"), mac)
  check(a.Text("macaddress"), mac)
  check(a.Text("timestamp"), timestamp)
  peri := a.Text("periodic")
  if peri == "" { peri = "none" }
  if periodic == "" { periodic = "none" }
  siFail(peri, periodic)
  check(a.Text("headertag"), headertag)
  if status != "error" { // if status==error, result contains plaintext message
    check(a.Text("result"), "none")
  }
  
  // The strange Join/Fields combo gets rid of the whitespace which gosa-si introduces into xmlmessage
  xmlmessage_txt := strings.Join(strings.Fields(a.Text("xmlmessage")),"")
  siFail(xmlmessage_txt, a.Text("xmlmessage"))
  decoded, _ := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(a.Text("xmlmessage")),""))
  xmlmessage, err := xml.StringToHash(string(decoded))
  check(err, nil)
  if err == nil {
    check(checkTags(xmlmessage, "header,source,target,timestamp,periodic?,macaddress"), "")
    check(xmlmessage.Text("header"), "job_" + headertag)
    check(xmlmessage.Text("source"), "GOSA")
    check(xmlmessage.Text("target"), mac)
    check(xmlmessage.Text("timestamp"), timestamp)
    peri = xmlmessage.Text("periodic")
    if peri == "" { peri = "none" }
    check(peri, periodic)
    check(xmlmessage.Text("macaddress"), mac)
  }
}


// Like check() but expects the test to fail if the daemon was not launched
// by the system test but was started beforehand.
func nonLaunchedFail(x interface{}, expected interface{}) {
  if launched_daemon { 
    checkLevel(x, expected,2) 
  } else {
    checkFailLevel(x, expected,2)
  }
}

// Like check() but expects the test to fail if testing a gosa-si instead of go-susi.
func siFail(x interface{}, expected interface{}) bool {
  if gosasi {
    return checkFailLevel(x, expected,2)
  } else {
    return checkLevel(x, expected,2)
  }
  return true
}
