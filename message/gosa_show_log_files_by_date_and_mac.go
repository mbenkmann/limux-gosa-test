/*
Copyright (c) 2013 Landeshauptstadt München
Author: Matthias S. Benkmann

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
         "fmt"
         
         "../xml"
         "../config"
       )

// Handles the message "gosa_show_log_files_by_date_and_mac".
//  xmlmsg: the decrypted and parsed message
// Returns:
//  unencrypted reply
func gosa_show_log_files_by_date_and_mac(xmlmsg *xml.Hash) string {
  header := "show_log_files_by_date_and_mac"
  return fmt.Sprintf("<xml><header>%v</header><%v></%v><source>%v</source><target>GOSA</target><session_id>1</session_id></xml>",header,header,header,config.ServerSourceAddress)
}
