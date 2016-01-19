<table summary="{t}Workstation startup{/t}" style="width:100%;">
 <tr>
  <td style='width:50%; '>

   <table summary="{t}Boot parameters, LDAP server, Kernel Modules and Shares{/t}">

   <!--
    - Boot parameters 
    -->
   <tr><td><h3>{t}Boot parameters{/t}</h3></td></tr>
    
    {if $fai_activated && $si_active && !$si_fai_action_failed}
    <tr>
     <td>
      <label for="gotoBootKernel">{t}Boot kernel{/t}</label>
     </td>
     <td style="width:70%">
      {render acl=$gotoBootKernelACL}
      <select id="gotoBootKernel" name="gotoBootKernel" size=1>
       {html_options options=$gotoBootKernels selected=$gotoBootKernel}
      </select>
      {/render}
     </td>
    </tr>
    {/if}


   <!--
    - Kernel parameters 
    -->
    <tr>
     <td>
      <label for="gotoKernelParameters">{t}Custom options{/t}</label>
     </td>
     <td>
      {render acl=$gotoKernelParametersACL}
       <input name="gotoKernelParameters" id="gotoKernelParameters" 
          size=25 maxlength=500 value="{$gotoKernelParameters}"  type='text'
          title="{t}Enter any parameters that should be passed to the kernel as append line during boot up{/t}">
      {/render}
     </td>
    </tr>


   <!--
    - LDAP servers  
    -->
    <tr>
     <td colspan="2">
      <hr><h3><label for="gotoLdapServer">{t}LDAP server{/t}</label></h3>
      {render acl=$gotoLdapServerACL}
        {if $member_of_ogroup}
          (<input type='checkbox' name='gotoLdap_inherit' {if $gotoLdap_inherit} checked {/if} value="1"
            onClick="document.mainform.submit();" class='center'>
            &nbsp;{t}inherit from group{/t}
          )
        {/if}
      {/render}
      {render acl=$gotoLdapServerACL_inherit}
        {$gotoLdapServers}	
      {/render}

      {render acl=$gotoLdapServerACL_inherit}
        <select name='ldap_server_to_add' id='ldap_server_to_add' size="1" style="max-width:480px">
         {html_options options=$gotoLdapServerList}	
        </select>
      {/render}

      {render acl=$gotoLdapServerACL_inherit}
        <button type='submit' name='add_ldap_server' id="add_ldap_server">
        {msgPool type=addButton}</button>
      {/render}

     </td>
    </tr>


   <!--
    - Kernel modules
    -->
    <tr>
     <td colspan="2">
      <hr/><h3>{t}Kernel modules (format: name parameters){/t}</h3>
      {render acl=$gotoModulesACL}
       <select style="width:100%; height:100px;" name="modules_list[]" size=15 multiple 
         title="{t}Add additional modules to load on startup{/t}">
        {html_options values=$gotoModules output=$gotoModules}
        <option disabled>&nbsp;</option>
       </select>
      {/render}

      <br>
      {render acl=$gotoModulesACL}
       <input type='text' name="module" size=30 align=middle maxlength=30>
      {/render}

      {render acl=$gotoModulesACL}
       <button type='submit' name='add_module'>
      {msgPool type=addButton}</button>
       &nbsp;
      {/render}

      {render acl=$gotoModulesACL}
       <button type='submit' name='delete_module'>
       {msgPool type=delButton}</button>
      {/render}
     </td>
   </tr>


   <!--
    - Shares
    -->
    <tr>
     <td colspan="2">
      <hr/><h3><label for="gotoShare">{t}Shares{/t}</label></h3>
    
      {render acl=$gotoShareACL}
       <select style="width:100%;height:100px;" name="gotoShare" multiple size=4 id="gotoShare">
        {html_options values=$gotoShareKeys output=$gotoShares}
        <option disabled>&nbsp;</option>
       </select>
      {/render}

      <br>
   
      {render acl=$gotoShareACL}
       <select name="gotoShareSelection" size=1>
        {html_options values=$gotoShareSelectionKeys output=$gotoShareSelections}
        <option disabled>&nbsp;</option>
       </select>
      {/render}

      {render acl=$gotoShareACL}
       <input type="text" size=15 name="gotoShareMountPoint" value="{t}Mount point{/t}">
      {/render}

      {render acl=$gotoShareACL} 
       <button type='submit' name='gotoShareAdd'>
       {msgPool type=addButton}</button>
      {/render}

      {render acl=$gotoShareACL}
       <button type='submit' name='gotoShareDel' {if $gotoSharesCount == 0} disabled {/if}>{t}Remove{/t}</button>
      {/render}

     </td>
    </tr>

   </table>

  </td>
  <td>


   <!--
    - FAI classes
    -->
  <td class='left-border'>
   {if !$fai_activated}
    <h3>{t}FAI Object assignment disabled. You can't use this feature until FAI is activated.{/t}</h3>
   {elseif !$si_active}
    <b>{t}GOsa support daemon not configured{/t}</b>
    <br>
    {t}FAI settings cannot be modified{/t}
   {elseif $si_fai_action_failed}

    <b>{msgPool type=siError}</b>
    <br>
   
    {t}Check if the GOsa support daemon (gosa-si) is running.{/t}
    <button type='submit' name='fai_si_retry'>
    {t}retry{/t}</button>
   
   {elseif $fai_activated}
   
   <table summary="{t}FAI class assignment{/t}">
    <tr>
     <td style='width:75%'>
      <b>{t}FAI server{/t}</b>
     </td>
     <td style='width:25%'>
      <b>{t}Release{/t}</b>
     </td>
    </tr>
    <tr>
     <td>
      {render acl=$FAIdebianMirrorACL}
        {if $member_of_ogroup}
          <input type='hidden' name='FAIdebianMirror_inherit_submitted' value='1'>
          <input type='checkbox' name='FAIdebianMirror_inherit' {if $FAIdebianMirror_inherit} checked {/if} value="1"
            onClick="document.mainform.submit();" class='center'> {t}Inherit from object group{/t}
        {/if}
        
        <input type='hidden' name='FAIdebianMirror_auto_submitted' value='1'>
        <input type='checkbox' name='FAIdebianMirror_auto' {if $FAIdebianMirror_inherit} disabled {/if} {if $FAIdebianMirror_auto} checked {/if} value="1"
        onClick="document.mainform.submit();" class='center'> {t}Choose automatically{/t}
      {/render}
     </td>
     <td>
      {render acl=$FAIreleaseACL}
      {if $FAIdebianMirror_inherit}
      <select name="FAIrelease"  disabled size=1>
          {html_options options=$InheritedFAIrelease output=$InheritedFAIrelease selected=$InheritedFAIrelease}
         </select>
         {else}
      
       <select name="FAIrelease"  onchange='document.mainform.submit()' size=1>
         {html_options values=$available_fai_releases output=$available_fai_releases selected=$FAIrelease}	
       </select>
       {/if}
      {/render}
     </td>
    </tr>
    <tr>
     <td colspan="2">
      {render acl=$FAIdebianMirrorACL}
         {$FAIdebianMirrorList}

      <!--<select name='FAIdebianMirror_to_add' id='FAIdebianMirror_to_add' size="1" style="max-width:480px" {if $FAIdebianMirror_inherit || $FAIdebianMirror_auto} disabled {/if} > -->
      <select name='FAIdebianMirror_to_add' id='FAIdebianMirror_to_add' size="1" style="max-width:480px" {if $FAIdebianMirror_inherit || $FAIdebianMirror_auto} disabled {/if} > 
       {html_options values=$availableFAIdebianMirror output=$availableFAIdebianMirror}	
      </select>
      <button type='submit' name='add_FAIdebianMirror' id='add_FAIdebianMirror' {if $FAIdebianMirror_inherit || $FAIdebianMirror_auto} disabled {/if} >
      {msgPool type=addButton}</button>
      {/render}
     </td>
    </tr>
           

   <!--
    - assigned FAI classes
    -->
    <tr>
     <td colspan="2"> 
       <hr/><h3>{t}Assigned FAI classes{/t}</h3>
       
       {render acl=$FAIclassACL}
        {$FAIScriptlist}	
       {/render}
       
       {if !$FAIdebianMirror_inherit}
       
       {render acl=$FAIclassACL}
        <select name="FAIclassesSel" size=1>
         {foreach from=$FAIclasses item=val key=key}
          <option value="{$key}">{$key}&nbsp;[{$val}]</option>
         {/foreach}
        </select>
       {/render}

       {render acl=$FAIclassACL}
        <button type='submit' name='AddClass'>{msgPool type=addButton}</button>
       {/render}
      
       {/if}

     </td>
    </tr>
   </table>

{/if} <!-- FAI active-->
  </td>
 </tr>
</table>

<input name="WorkstationStarttabPosted" type="hidden" value="1">

<script language="JavaScript" type="text/javascript">
<!-- // First input field on page
focus_field('gotoLdapServer');
-->
</script>

