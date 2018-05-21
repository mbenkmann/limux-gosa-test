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
         
         "../db"
         "github.com/mbenkmann/golib/util"
       )

var originFAIclassRegexp = regexp.MustCompile(`^\s*LHMoriginFAIclass\s*=\s*'([^']+)'\s*$`)
var faiClassStringRegexp = regexp.MustCompile(`^\s*\+\s*FAIclass string\:\s*(.*)$`)

// exported only for system-tests (from a different package)
func Postprocess_logfiles(logdir string, macaddress string) {
  postprocess_logfiles(logdir, macaddress)
}

// Performs a postprocessing on logfiles that the server
// recived in clmsg_save_fai_log. We currently use this method
// to update the LDAP attributes FAIlastrunClass and FAIlastrunTime
// with values that we did extract from submitted logfiles.
//
//  logdir: path of the directory that contains the logfiles
//  macaddress: macaddress of the client that produced the logfiles
//
func postprocess_logfiles(logdir string, macaddress string) {

  // faiLastrunTime should be updated with the epoch timestamp of now.
  // This is done for successfull and failed FAI runs.
  faiLastrunTime := strconv.FormatInt(time.Now().Unix(), 10)

  // Fill faiLastrunClass with the value of FAIclass that was used
  // for this FAI run. This is only done if the FAI run was successfull.
  // In case of an error, we leafe faiLastrunClass empty, so that
  // it gets removed in the end.
  //
  // To retrieve the value of FAIclass that was used for this FAI run,
  // we extract the varialbe "LHMoriginFAIclass" from variables.log:
  // Ldap2fai has created this variable for us. It holds the origin
  // content of the FAIclass before the fai-classes were expanded
  // for FAI. FAI itself only uses the expanded list of fai-classes
  // and doesn't need or read LHMoriginFAIclass.
  //
  // A successfull FAI run is identified by the fact, that FAIstate
  // doesn't contain an error message. A successfull FAI run is given,
  // if FAIstate is either "softupdate", "install" or "localboot".
  // Note, that the caller (clmsg_save_fai_log) just did start a
  // "set progress 100" job in parallel and this job could have set
  // faistate to "localboot".
  faiLastrunClass := ""
  faistate := db.SystemGetState(macaddress, "FAIstate")
  if (faistate == "softupdate" || faistate == "install" || faistate == "localboot") {

    dat, err := ioutil.ReadFile(path.Join(logdir, "variables.log"))
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
    if(faiLastrunClass == "") {
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
    }
    // END Workaround

  }
  
  // update database
  util.Log(1, "INFO: setting FAIlastrunClass='%v' and FAIlastrunTime='%v'", faiLastrunClass, faiLastrunTime)
  if(faiLastrunClass != "") {
    db.SystemSetState(macaddress, "FAIlastrunClass", faiLastrunClass)
  } else {
    db.SystemSetStateMulti(macaddress, "FAIlastrunClass", []string{}) // removes FAIlastrunClass
  }
  db.SystemSetState(macaddress, "FAIlastrunTime", faiLastrunTime)
}
