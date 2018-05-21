/* 
Copyright (c) 2013 Matthias S. Benkmann

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
         "bufio"
         "os"
         "os/exec"
         "path"
         "time"
         "regexp"
         "strings"
         
         "../db"
         "../xml"
         "github.com/mbenkmann/golib/util"
         "github.com/mbenkmann/golib/bytes"
         "../config"
         "../security"
       )

var actionRegexp = regexp.MustCompile("^[_a-zA-Z-]+$")

func match(data []byte, i int, s string) bool {
  for k := range s {
    if i+k == len(data) { return false }
    if data[i+k] != s[k] { return false }
  }
  return true
}

// Handles the message "CLMSG_save_fai_log".
//  buf: the decrypted message
//  context: the security context
func clmsg_save_fai_log(buf *bytes.Buffer, context *security.Context) {
  macaddress := ""
  action := ""
  start := 0
  end := 0
  data := buf.Bytes()
  for i := 0; i < len(data)-19; i++ {
    if data[i] == '<' {
      if i+12+17 <= len(data) && match(data, i, "<macaddress>") {
        macaddress = string(data[i+12:i+12+17])
      } else 
      if match(data, i, "<fai_action>") {
        for k := i + 12; k < len(data); k++ {
          if data[k] == '<' {
            action = string(data[i+12:k])
            i = k
            break
          }
        }
      } else
      if match(data, i, "<CLMSG_save_fai_log>") {
        start = i+20
      } else
      if match(data, i, "</CLMSG_save_fai_log>") {
        end = i
      }
    }
  }

  if !macAddressRegexp.MatchString(macaddress) {
    util.Log(0, "ERROR! CLMSG_save_fai_log with illegal <macaddress> \"%v\"",macaddress)
    return
  }
  
  if !actionRegexp.MatchString(action) {
    util.Log(0, "ERROR! CLMSG_save_fai_log with illegal <fai_action> \"%v\"",action)
    return
  }
  
  if action != "audit" {
    util.Log(1, "INFO! Received log files from client %v. Assuming CLMSG_PROGRESS 100", macaddress)
    progress_msg := xml.NewHash("xml","CLMSG_PROGRESS","100")
    progress_msg.Add("macaddress", macaddress)
    clmsg_progress(progress_msg)
  }
  
  timestamp := util.MakeTimestamp(time.Now())
  logname := action+"_"+timestamp[0:8]+"_"+timestamp[8:]
  logdir := path.Join(config.FAILogPath, strings.ToLower(macaddress), logname)
  
  // NOTE: 1kB = 1000B, 1kiB = 1024B
  util.Log(1, "INFO! Storing %vkB of %v log files from %v in %v",len(data)/1000, action, macaddress, logdir)
  
  err := os.MkdirAll(logdir, 0755)
  if err != nil {
    util.Log(0, "ERROR! Error creating log directory \"%v\": %v", logdir, err)
    return
  }
  
  // Create convenience symlink with the system's name as alias for MAC address.
  go util.WithPanicHandler(func() {
    if plainname := db.SystemPlainnameForMAC(macaddress); plainname != "none" {
      linkpath := path.Join(config.FAILogPath, strings.ToLower(plainname))
      link_target, err := os.Readlink(linkpath)
      if err != nil && !os.IsNotExist(err.(*os.PathError).Err) {
        util.Log(0, "ERROR! %v exists but is not a symlink: %v", linkpath, err)
        return
      }
      if err == nil {
        if link_target == strings.ToLower(macaddress) {
          return // symlink is already correct => nothing to do
        }
        
        util.Log(0, "WARNING! Machine %v has a new MAC %v . Removing old symlink %v => %v", plainname, macaddress, linkpath, link_target)
        err = os.Remove(linkpath)
        if err != nil {
          util.Log(0, "ERROR! Removing %v failed: %v", linkpath, err)
          // Don't bail out. Maybe we can create the new symlink anyway.
        }
      }
      err = os.Symlink(strings.ToLower(macaddress), linkpath)
      if err != nil && !os.IsExist(err.(*os.LinkError).Err) {
        util.Log(0, "ERROR! Could not create symlink %v => %v: %v", linkpath, strings.ToLower(macaddress), err)
      }
    }
  })

  files := []int{}
  for i := start; i < end; i++ {
    if data[i] == ':' && match(data, i-8, "log_file") {
      k := i
      i++
      for i < end { 
        if data[i] == ':' { 
          if k+1 < i { files = append(files, k+1, i) }
          break 
        }
        i++
      }
    }
  }
  
  files = append(files, end+8)
  fcount := context.Limits.MaxLogFiles
  if fcount <= 0 { fcount = 999999 }
  
  for i := 0; i < len(files)-1; i+=2 {
    if fcount--; fcount < 0 {
      util.Log(0, "WARNING! [SECURITY] %v has sent more than the %v log files allowed by certificate => Ignoring excess files", context.PeerID.IP, context.Limits.MaxLogFiles)
      break
    }
    
    fname := string(data[files[i]:files[i+1]])
    logdata := data[files[i+1]+1:files[i+2]-8]
    util.Log(1, "INFO! Processing \"%v\" (%vkB)", fname, len(logdata)/1000)
    
    logdata = util.Base64DecodeInPlace(logdata)
    
    // As a precaution, make sure fname contains no slashes.
    fname = strings.Replace(fname,"/","_",-1)
    err = ioutil.WriteFile(path.Join(logdir, fname), logdata, 0644)
    if err != nil {
      util.Log(0, "ERROR! Could not store \"%v\": %v", path.Join(logdir, fname), err)
      continue
    }
  }

  postprocess_logfiles(logdir, macaddress)
}

// Executes program and reads from its standard output log files to transfer to
// the target server. See fai-savelog-hook in the manual.
// hookname is the name of the hook for use in log messages.
func Send_clmsg_save_fai_log(target string, program string, hookname string) {
  var buffy bytes.Buffer
  defer buffy.Reset()
  
  clientpackageskey := config.ModuleKey["[ClientPackages]"]
  // If [ClientPackages]/key missing, take the last key in the list
  // (We don't take the 1st because that would be "dummy-key").
  if clientpackageskey == "" { clientpackageskey = config.ModuleKeys[len(config.ModuleKeys)-1] }
  
  util.Log(1, "INFO! Launching %v %v", hookname, program)
  start := time.Now()
  env := config.HookEnvironment()
  cmd := exec.Command(program)
  cmd.Env = append(env, os.Environ()...)

  out, err := cmd.StdoutPipe()
  if err != nil {
    util.Log(0, "ERROR! Could not get stdout pipe for %v: %v", program, err)
    return
  }
  defer out.Close()
  
  in, err := cmd.StdinPipe()
  if err != nil {
    util.Log(0, "ERROR! Could not get stdin pipe for %v: %v", program, err)
    return
  }
  defer in.Close()

  err = cmd.Start()
  if err != nil {
    util.Log(0, "ERROR! Could not launch %v: %v", program, err)
    return
  }
  
  buffy.WriteString("<xml><header>CLMSG_save_fai_log</header><source>")
  buffy.WriteString(config.ServerSourceAddress)
  buffy.WriteString("</source>")
  buffy.WriteString("<target>")
  buffy.WriteString(target)
  buffy.WriteString("</target>")
  buffy.WriteString("<macaddress>")
  buffy.WriteString(config.MAC)
  buffy.WriteString("</macaddress>")
  buffy.WriteString("<CLMSG_save_fai_log>")
  
  reader := bufio.NewReader(out)
  fai_action := ""
  for {
    line, err := reader.ReadString('\n')
    if err != nil {
      util.Log(0, "ERROR! Error reading stdout from %v: %v", program, err)
      return
    }

    line = strings.TrimSpace(line)
    if line == "install" || line == "softupdate" || line == "audit" {
      fai_action = line
      break
    }
    
    buffy.WriteString(line)
  }
  
  util.Log(1, "INFO! Received %v bytes in %v from %v", buffy.Len(), time.Since(start), hookname)
  
  buffy.WriteString("</CLMSG_save_fai_log>")
  buffy.WriteString("<fai_action>")
  buffy.WriteString(fai_action)
  buffy.WriteString("</fai_action>")
  buffy.WriteString("</xml>")
  
  util.Log(1, "INFO! Sending %v bytes of log files to %v", buffy.Len(), target)
  security.SendLnTo(target, buffy.String(), clientpackageskey, false)
  
  in.Write([]byte{'\n'}) // notify hook that transfer is complete
}
