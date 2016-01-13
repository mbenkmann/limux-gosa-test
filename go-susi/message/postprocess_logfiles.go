/* 
Copyright (c) 2015 Landeshauptstadt München

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, 
MA  02110-1301, USA.
*/

package message

import (
         "io/ioutil"
         "path"
         "regexp"
         "strings"
         "time"
         "strconv"
         
         "../util"
         "../db"
       )

var originFAIclassRegexp = regexp.MustCompile(`^\s*LHMoriginFAIclass\s*=\s*'([^']+)'\s*$`)
var faiClassStringRegexp = regexp.MustCompile(`^\s*\+\s*FAIclass string\:\s*(.*)$`)

// exported only for system-tests (from a different package)
func Postprocess_logfiles(logdir string, macaddress string) {
  postprocess_logfiles(logdir, macaddress)
}

// Performs a postprocessing on logfiles that the server 
// recived in clmsg_save_fai_log.
//  logdir: path of the directory that contains the logfiles
//  macaddress: macaddress of the client that produced the logfiles
func postprocess_logfiles(logdir string, macaddress string) {

  // we do the following steps only in case of successfull FAI runs.
  // We identify a successfull FAI run if FAIstate doesn't contain
  // an error message. But we can be more specific: The FAIstate for
  // a successfull FAI run could either be softupdate, install or
  // localboot (if the in parallel running job "set progress 100"
  // was already processed). Note, that the caller (clmsg_save_fai_log)
  // just did start this "set progress 100" job in parallel. 
  // This job sets faistate to "localboot".
  faistate := db.SystemGetState(macaddress, "FAIstate")
  if !(strings.HasPrefix(faistate, "softupdat") || strings.HasPrefix(faistate, "install") || faistate == "localboot") {
    util.Log(1, "DEBUG! nothing to do in postprocess_logfiles for faistate '%v'", faistate)
    return
  }

  faiLastrunTime := strconv.FormatInt(time.Now().Unix(), 10)

  // Extract the varialbe "LHMoriginFAIclass" from variables.log:
  // Ldap2fai has created this variable for us. It holds the origin
  // content of the FAIclass before the fai-classes were expanded
  // for FAI. FAI itself only uses the expanded list of fai-classes
  // and doesn't need or read LHMoriginFAIclass. Ldap2fai exports
  // the variable solely for the purpose to be read here.
  dat, err := ioutil.ReadFile(path.Join(logdir, "variables.log"))
  faiLastrunClass := ""
  if err == nil {
    for _,line := range strings.Split(string(dat), "\n") {
      res := originFAIclassRegexp.FindStringSubmatch(line)
      if(len(res) > 1) {
        faiLastrunClass = res[1]
        break
      }
    }
  } else {
    util.Log(1, "ERROR! could not read logfile variables.log: %v", err)
  }

  if(faiLastrunClass == "") {
    faiLastrunClass = "UNKNOWN_ORIGIN_FAI_CLASSES"

    // START Workaround:
    // The above creation of the LHMoriginFAIclass variable requires
    // a new version of ldap2fai on the clients and tramp clients don't
    // have got this version installed.
    // The following workaround can fill this gap until wanderer is
    // released everywhere. It will (by incident) practically work with
    // logfiles that are created by tramp clients. "by incident", because
    // it requires ldap2fai to be run with "-v" switch and because
    // we are parsing a log message from ldap2fai that was never intended
    // to be processed this way.
    // The workaround could be removed if wanderer is out there everywhere.
    dat, err = ioutil.ReadFile(path.Join(logdir, "ldap2fai.log"))
    if err == nil {
      for _,line := range strings.Split(string(dat), "\n") {
        res := faiClassStringRegexp.FindStringSubmatch(line)
        if(len(res) > 1) {
          faiLastrunClass = res[1]
          break
        }
      }
    } else {
      util.Log(1, "ERROR! could not read (fallback) logfile ldap2fai.log: %v", err)
    }
    // END Workaround
  }
  
  // update database
  util.Log(2, "INFO: setting FAIlastrunClass='%v' and FAIlastrunTime='%v'", faiLastrunClass, faiLastrunTime)
  db.SystemSetState(macaddress, "FAIlastrunClass", faiLastrunClass)
  db.SystemSetState(macaddress, "FAIlastrunTime", faiLastrunTime)
}
