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
         "../xml"
         "../config"
       )

// Handles the message "gosa_get_log_file_by_date_and_mac".
//  xmlmsg: the decrypted and parsed message
// Returns:
//  unencrypted reply
func gosa_get_log_file_by_date_and_mac(xmlmsg *xml.Hash) *xml.Hash {
  header := "get_log_file_by_date_and_mac"
  x := xml.NewHash("xml","header",header)
  x.Add(header)
  x.Add("source", config.ServerSourceAddress)
  x.Add("target", "GOSA")
  x.Add("session_id","1")
  return x
}
