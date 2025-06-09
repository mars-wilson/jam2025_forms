

# requires -module $ENV:ScriptsRoot\include\database\dbutils
# requires -module $ENV:ScriptsRoot\Utility

param(
    [Parameter(mandatory=$false)] [switch] $Go,
    [Parameter(mandatory=$false)] [switch] $Testing,
    [Parameter(mandatory=$false)] [string] $TestingTo = '--redacted---@warren-wilson.edu',
    [Parameter(mandatory=$false)] $ERPDatabase = 'TMSEprd' 
)

if ($Testing) { write-host "Test Mode!" }


# this custom module contains cmdlets to send email and do other account things.
import-module $ENV:ScriptsRoot\Utility -Force -DisableNameChecking
import-module $ENV:ScriptsRoot\include\database\dbutils -Force -DisableNameChecking

<#
Each task has a process script:  Process-FormName.
The name of the funciton must match exactly the name of the form posted to the table by the formflow form.
   With the exception that the form name on the form on JICS can have spaces in it which are removed.
For example, the form named "Name Update" which is posted to the formName field in Data Mappings
    will cause process-NameUpdate to be run.

process-<FormName> needs to call notifiy-form to queue any emails or notifications to send out.
#>



<#
This is the complete code for making changes to the dynamic approvers and notifications table
#>
function process-ApproverUpdate {
    param(
        [Parameter(Position=0,mandatory=$true)] $formRecord
    )
    # $Testing and $ERPDatabase are globals.

    # trimming is important.  The form data may have a lot of extra S P A C E.
    write-host "About to process-ApproverUpdate - load params"
    $r = @{
        id_num       = [string]($formRecord.id_num)  # This is the submitter ID_num.  They may not be a manager.
        formName     = $(if (!$formRecord.string01) {''} else { ($formRecord.string01).trim() })
        userEmail    = $(if (!$formRecord.string02) {''} else { ($formRecord.string02).trim() })
        approverType = $(if (!$formRecord.string03) {''} else { ($formRecord.string03).trim() })
        action       = $(if (!$formRecord.string04) {''} else { ($formRecord.string04).trim() })
        extraEmail   = $(if (!$formRecord.string05) {''} else { ($formRecord.string05).trim() })
    }
    write-host "params: " $r
    
    $ErrMsg = ""

    # first see if the submitter has rights to this form - a manager or in the ITS office.
    $result = invoke-sqlquery -database $ERPDatabase -sqlParameters $r  -as PSObjectArray -query  "
        select  formName from _wwn_myWWC_forms_approvers
        where formname = @formName and (
            (approverType = 'manager' and id_num = @id_num) 
            or (@id_num in (select id_num from wwn_mgmt..wwn_accounts where recordstatus='C' and ad_groups like '%ITS Office%'))
            )   "
    if (!$result) {
        $ErrMsg =  "The user $($r.id_num) does not have manager permissions for form $($r.formName)"
    }

    if (!$ErrMsg) {
        # Get the ID num for the useremail.  It's okay if this is a historical record.
        $result = invoke-sqlquery -database 'wwn_mgmt' -sqlParameters $r -As PSObjectArray -query  "
        select top 1 id_num, * from wwn_accounts  where 
        ga_email = @userEmail or ad_email = @userEmail order by updated desc"
        if ($result) {
            $r.user_idnum = $result.id_num
            write-host "id_num of user " $r.userEmail " being managed is: " $result.id_num
        }
        else {
            $ErrMsg = "ID Number for forms user email $($r.userEmail) cannot be determined."
        }
    }

    if (!$ErrMsg -and ($r.action -like 'remove')) {
        # Remove an entry
        $result = invoke-sqlquery -database $ERPDatabase -sqlParameters $r -query  "
            select top 1 id, formName, a.id_num,  extraEmail, approverType 
            from _wwn_myWWC_forms_approvers a where a.id_num = @user_idnum"

        if ($result) {
            $Message = "action Remove  for user $($r.userEmail) id $($r.user_idnum) from Forms Approvers:
            formName            = $($result.FormName)
            id_num (submitter)  = $($result.id_num)
            approverType (role) = $($result.approverType)
            extraEmail (cc)     = $($result.extraEmail)"

            write-host $Message
            if ($Testing) {
                write-host "Would:" 
            }
            else {
                invoke-sqlquery  -database $ERPDatabase -sqlParameters $r -query  "
                delete from  _wwn_myWWC_Forms_approvers where id = @user_idnum "
            }

            write-host "    delete from  _wwn_myWWC_Forms_approvers where id = @user_idnum  | @user_idnum=$($r.userEmail.id_num)"
        }
        else {
            write-host "Cannot remove row from the form approvers table - it appears to be missing.
            Would want to Remove for user $($r.userEmail) from Forms Approvers:
            formName            = $($r.formName)
            id_num (submitter)  = $($r.user_idnum)
            approverType (role) = $($r.approverType)
            extraEmail (cc)     = $($r.extraEmail)"
            write-host "No error reported to end user because it might be removed twice?"
            return  $True  # Mark the record to be processed. But there is data to return.
        }
    }
    elseif (!$ErrMsg) {
        $query = "
            IF EXISTS (
                SELECT 1 FROM _wwn_myWWC_Forms_approvers 
                WHERE FormName = @formName 
                AND id_num = @user_idnum
            )
            BEGIN
                -- Update existing record
                UPDATE _wwn_myWWC_Forms_approvers
                SET approverType = @approverType, 
                    extraEmail = @extraEmail, 
                    status = 'A'
                WHERE FormName = @formName 
                AND id_num = @user_idnum
            END
            ELSE
            BEGIN
                -- Insert new record
                INSERT INTO _wwn_myWWC_Forms_approvers (FormName, id_num, approverType, extraEmail, status) 
                VALUES (@formName, @user_idnum, @approverType, @extraEmail, 'A')
            END"
        if ($Testing) {
            write-host "Would:  "
        }
        else {
            invoke-sqlquery  -database $ERPDatabase -sqlParameters $r -query $query
        }
        write-host "$query | `n  #user_idnum=$($r.user_idnum) @formName=$($r.formName) @approverType=$($r.approverType)   @extraEmail=$($r.extraEmail)"
        
        $Message = "Update Successful for user $($r.user_idnum) with email $($r.userEmail) form name $($r.formName)  $($r.approverType) for recipient $($r.extraEmail)"
    }

    if ($ErrMsg) {
        notify-Form -formName 'Approver Update' -Subject 'Approver Update - Error in Update' -Message "
        `nOoops!  `n`n   We were not able to make the requested approver update change:  `n`n`n$ErrMsg`n`nThanks,`nITS wwc_forms"
    }
    else {
        notify-Form -formName 'Approver Update' -message $Message
    }
    $r  # Return the successful or failed update data.
}



# Process to handle check-in of a student.
# This allows the student to set their own checkin date.
# Could be a stored procedure for sure.
function process-CheckInDate {
    param(
        [Parameter(Position=0,mandatory=$true)] $formRecord
    )
    $r = $true
    $id_num =$formRecord.id_num
    $checkInDate = $formRecord.date1
    $query = "update stud_sess_assign set RESIDENCE_HALL_CHECKIN_DTE=@checkInDate
    where ID_NUM = @id_num and (SESS_CDE = (select top 1 session from WWN_curr_prev_next_terms_View 
            where kind='ug_major' 
            and trm_end_dte > getdate()
            order by trm_end_dte))"

    $params =  @{id_num= $id_num; checkInDate= $checkInDate}
    try {
        $null = invoke-sqlquery -database $ERPDatabase -sqlParameters $params -query $query
    }
    catch {
        write-host "ERROR! ERROR! process-CheckInDate:  $_"
        $r = $false
    }
return $r
}


<# 

This form, "Name Update", is used for community members to update their name (and personal email addresses as it turns out)
It needs to be updated to change the name in our LMS (Moodle) - the Moodle LMS API is kinda wonky and incomplete
So some direct database updates to Moodle may be necessary.

The script uses GAMADV to update Google Workspace.

#>
function process-NameUpdate {
    param(
        [Parameter(Position=0,mandatory=$true)] $formRecord
    )

    write-verbose " Getting user $($r.id_num)"
    $u = get-user $formRecord.id_num -content 'google;ad;ex'

    # trimming is important.  The form data may have a lot of extra S P A C E.
    # But you can't trim a NULL.  So be careful with that!
    $r = @{
        id_num = [string]($formRecord.id_num)
        cname  = $(if ($formRecord.string01) {($formRecord.string01).trim()} else {$u.ex_first_name})
        pname  = $(if ($formRecord.string02) {($formRecord.string02).trim()} else {$u.ex_preferred_name})
        emlp   = $(if ($formRecord.string03) {($formRecord.string03).trim()} else {$ex_emlp})
    }

    if ($u.EX_Preferred_Name -like $r.pname -and $u.EX_First_Name -like $r.cname -and $u.EX_EMLP -like $r.emlp -and
        $u.AD_FirstName -like $r.cname  -and $u.GA_Name -like "*, $($r.cname)" <# maybe query google first name somehow? #> ) {
        write-host "Update not needed - no changes for $($r.id_num) $($r.cname) $($r.pname) pref: $($r.pname) $($r.emlp)"
        $r 
        return
    }

    $newName =  $r.cname + ' ' + $u.EX_Last_Name
    write-host "process-NameUpdate 
    Current Preferred: $($u.EX_Preferred_Name)  new:  $($r.pname)
    Current Campus:    $($u.EX_First_Name) new:  $($r.cname)
    Current EMLP:      $($u.EX_EMLP)  new:  $($r.emlp)
    Current AD:        $($u.AD_FirstName)  new:  $($r.cname)
    Current GA:        $($u.GA_Name)  new:  $($newName)
    "

    # fill in any missing data
    if (!($r.cname)) { $r.cname = $u.EX_First_Name }
    if (!($r.pname)) { $r.pname = $u.EX_Preferred_Name }
    if (!($r.emlp))  { $r.emlp  = $u.EX_EMLP }


    $EmailMessage = "

Dear $($r.cname),

We have updated your campus name, community name, and personal email as follows:

Campus Name:  $($r.cname)
    Your campus name is a name other than the legal name and may not be used to avoid 
    legal obligation or with the intention to misrepresent. 
    This name is considered public directory information and can be viewed by family 
    members, employees of the College, and/or the general public.

Community Name: $($r.pname)
    Your community name is distinct from your campus name. 
    This name is not considered public directory information and is limited to select 
    rosters used for internal Warren Wilson College use. 
    Please note, that community name is not private as there is no way to ensure that 
    it remains confidential.

Personal Email:  $($r.emlp)
    Your personal email address will be used to reach you in the event that access 
    to your Warren Wilson email address is not working, and may also be used to reach 
    you after you've left Warren Wilson College.

Your name in Moodle:  
    Note that you can, at any time, log in to Moodle and change your first name.
    You have complete control over your name there. Just go to your profile
    in the top right after logging in, and choose 'Edit Profile.' This system
    does not change your name in Moodle for you.

Please make sure to reference the Name/Name Change Policies and Procedures page on myWWC
to reference where your legal name, campus name, and community name are used.

If you have any questions, please reach out the IT office at:
    --redacted---@warren-wilson.edu
    828-771-3094

    "
     

    $query = "
        update address_master set addr_line_1 = @emlp where id_num = @id_num and addr_cde = 'emlp';
        update name_master set first_name = @cname, preferred_name = @pname, job_name = 'name_update', job_time=CURRENT_TIMESTAMP 
        where id_num = @id_num
        "

    write-verbose "process_NameUpdate:`n$q parameters: @id_num = $($r.id_num)  @emlp = $($r.emlp) @cname = $($r.cname)  @pname = $($r.pname) "
    if ($Testing) {
        write-host "Would:  $query `n Parameters: @id_num = $($r.id_num)  @emlp = $($r.emlp) @cname = $($r.cname)  @pname = $($r.pname) "
    }
    else { 
        try {
            $null = invoke-sqlquery -database $ERPDatabase -sqlParameters $r -query $query
        }
        catch {
            write-host "ERROR! ERROR! process-nameUpdate:  $_"
            $rquery = $query.replace('@id_num', "'$($r.id_num)'").replace('@emlp', "'$($r.emlp)'").replace('@cname', "'$($r.cname)'").replace('@pname', "'$($r.pname)'")
            write-host "Query: $rquery"
            $r = $false
        }
    }

    # Attempt to update Google and AD
    try {

        if ($Testing) {
            write-host "Would: "
        }
        write-host "
            set-aduser  '$($u.AD_username)' -FirstName '$($r.cname)' -DisplayName '$newName'
            invoke-gam `"gam update user $($u.GA_Email) firstname `"$($r.cname)`"`"
            "
        if (!$Testing) {
            if ($u.ad_username) {
                $null = set-aduser $u.AD_username -GivenName $r.cname -DisplayName $newName
            }
            if ($u.GA_Email) {
                $null = invoke-gam "gam update user $($u.GA_Email) firstname `"$($r.cname)`""
            }
            $u = get-user $r.id_num
        }
    }
    catch {
        write-host "Something went wrong trying to change AD or GA names:  $_"
    }
    if ($r) {
        $To =  $formRecord.Email 
        write-host "notify-form -formName 'Name Update' -To $To  -Message $EmailMessage -Subject 'Name Updated Completed'"
        notify-form -formName 'Name Update' -To $To  -Message $EmailMessage -Subject 'Name Updated Completed'
    }
    $r

}


<# 
Our Finaid office routinley needs to do some corrections to fix financial aid action codes from PowerFaids.
Now they can self-serve this rather routine database operation from JICS
#>
function process-pf_wkstdyFixActionCode {
    param(
        [Parameter(Position=0,mandatory=$true)] $formRecord
    )

    # trimming is important.  The form data may have a lot of extra S P A C E.
    $r = @{
        dbname        = $formRecord.dbname
        submitter_id  = [string]($formRecord.id_num)
        id_num        = $(if (!$formRecord.string01) {''} else { ($formRecord.string01).trim() })
        lastname      = $(if (!$formRecord.string02) {''} else { ($formRecord.string02).trim() })
        newcode       = $(if (!$formRecord.string03) {''} else { ($formRecord.string03).trim() })
        poe_ID        = $(if (!$formRecord.string04) {''} else { ($formRecord.string04).trim() })
        fund_cde      = $(if (!$formRecord.string05) {''} else { ($formRecord.string05).trim() })
        dept          = $(if (!$formRecord.string06) {''} else { ($formRecord.string06).trim() })
    }
    write-host "About to  $($formRecord.FormName )  params $($r.id_num) $($r.lastname) $($r.newcode)"

    # let's validate first.
    $err = ''
    $acctDetails  = invoke-sqlquery -database 'tmseprd' -sqlparameters $r -as PSObjectArray -query "
        select * from wwn_mgmt.dbo.wwn_accounts where id_num = @id_num and recordstatus = 'C'"
    $existingDetails = invoke-sqlquery -database $r.dbname -sqlparameters $r -as PSObjectArray -query "
    select nm.id_num, nm.first_name, nm.last_name, 
        wkstdy_action_cde, pf_wrk_stdy.poe_id, fund_cde, wrk_stdy_dept, pos_start_dte,pos_end_dte,wkstdy_elig_amt, wkstdy_awd_hrs from pf_wrk_stdy
        join name_master nm on pf_wrk_stdy.id_num = nm.id_num
    where nm.id_num = @id_num and poe_ID = @poe_id and fund_cde = @fund_cde and wrk_stdy_dept = @dept
    "
    
    write-host "Action Code for  $($r.id_num)  $($existingDetails.first_name) $($existingDetails.last_name)  $($existingDetails.wrk_stdy_dept)
            before change: $($existingDetails.wkstdy_action_cde)"
    
    if (  (count($acctDetails)) -eq 0) {
        $err = "ID Num $($r.id_num) / POE_ID $($r.poe_ID) / Fund_cde $($r.fund_cde)  Combo   Not found."
    }
    elseif ($r.lastname -notlike "$($existingDetails.last_name)") {
        $err = "Name Mismatch.  Last Name is Incorrect for $($r.id_num) cannot change to $($r.newcode)"
    }

    if ($r.newcode -like '*remove*') {  # maybe someday provide a remove / delete functionality
        # Remove an entry
        $action = "Removed row "
    }
    else {
        $q = "update pf_wrk_stdy set wkstdy_action_cde = @newcode where id_num = @id_num and poe_id = @poe_id and fund_cde = @fund_cde and wrk_stdy_dept = @dept"
        invoke-sqlquery -database $ERPDatabase -sqlparameters $r -as PSObjectArray -query $q
        $action = $q -replace ('@id_num', $r.id_num) -replace ('@poe_id', $r.poe_id) `
            -replace ('@fund_cde', $r.fund_cde) -replace ('@newcode', $r.newcode) -replace ('@dept', $r.dept)
        write-host $action
        write-host "Change action done by $($r.submitter_id)"
    }
    
 
    #One of error or action is blank.
    $msg = "$($r.student_id)  $($existingDetails.first_name)   $($existingDetails.last_name)`n    " + $err + $action
    write-host $msg
    # $r.submitter_id  submitted the form.
    write-host "add-queuedNotification -FormName '$($formRecord.FormName)' -To '---redacted---@warren-wilson.edu'  -Subject `"pf_wkstdy  Changes`" -Body '$msg'"
    
    $SubmitterEmail = (get-user $($r.submitter_id) -content 'ad').AD_Email
    $SubmitterEmail += ';---redacted---@warren-wilson.edu'
    add-queuedNotification  -FormName $($formRecord.FormName)  -To $SubmitterEmail -Subject "pf_wkstdy Changes" -Body $msg
    $r  # returning someting is necessary (or false on failure)

}


<# 

This form is used by our campus post office (which doesn't routinely use J1) to update CPO box numbers for people on campus.

#>
function process-AssignCPOandCombo {
    param(
        [Parameter(Position=0,mandatory=$true)] $formRecord
    )

    # trimming is important.  The form data may have a lot of extra S P A C E.
    $r = @{
        submitter_id  = [string]($formRecord.id_num)
        # we should not get DBNull anymore because we are using -as PSObjectArray in query but column may be $null
        id_num        = $(if (!$formRecord.string01) {''} else { ($formRecord.string01).trim() })
        newbox        = $(if (!$formRecord.string02) {''} else { ($formRecord.string02).trim() })
        newcombo      = $(if (!$formRecord.string03) {''} else { ($formRecord.string03).trim() })
    }
    write-host "About to  $($formRecord.FormName )  params id: '$($r.id_num)' newbox: '$($r.newbox)'  newcombo: '$($r.newcombo)'"

    # let's validate first.
    $err = ''
    $acctDetails  = invoke-sqlquery -database 'tmseprd' -sqlparameters $r -as PSObjectArray -query "
        select * from wwn_mgmt.dbo.wwn_accounts where id_num = @id_num and recordstatus = 'C'"
    $existingDetails = invoke-sqlquery -database 'tmseprd' -sqlparameters $r -as PSObjectArray -query "
        select addr_line_1 as boxno, am.udef_10a_1 as combo,  first_name, last_name from name_master nm left join  address_master am 
            on am.id_num = nm.id_num and addr_cde = 'CPO' where nm.id_num = @id_num "
    
    write-host "Box for  id '$($r.id_num)'  $($existingDetails.first_name) $($existingDetails.last_name)
        before change: '$($existingDetails.boxno)'  '$($existingDetails.combo)'"
    
    $insertUpdateQuery = "
        if exists (select * from address_master
                where id_num = @id_num
                AND addr_cde = 'CPO')
        BEGIN
            UPDATE address_master set addr_line_1 = @newbox, udef_10a_1=@newcombo where addr_cde = 'CPO' and id_num = @id_num
            select 'UPDATED' as result
        END ELSE BEGIN
            INSERT INTO [ADDRESS_MASTER]
                (ID_NUM, ADDR_CDE, udef_10a_1, CASS_STS, DTE_CONFIRMED, STOP_ADDR_MAIL,
                    ADDR_PRIVATE, ADDR_LINE_1, PHONE_PRIVATE, EMAIL_ADDR, NOTIFICATION_ENABLED,
                    INSTITUTION_EMAIL, USER_NAME, JOB_NAME, JOB_TIME)
            VALUES
                (@id_num,'CPO', @newcombo, 'U',getdate(),'N',
                    'N',@newbox,'N','N','N',
                    'N','sysadmin-svc','wwc-forms-assign-cpo',getdate())
            select 'INSERTED' as result
        END
        "

    if ($r.newbox -like '*remove*') {
        write-host "Remove box  '$($existingDetails.boxno)' for id $($r.id_num)"
        if (  (count($acctDetails)) -eq 0) {
            $err = "person ID number was not found."
        }
        elseif (-not $existingDetails.boxno) {
            $err = "Action was remove but person has no box"
        }
        # Remove an entry
        #$q = " update address_master set addr_line_1 = '', udef_10a_1='' where addr_cde = 'CPO' and id_num = @id_num"
        if ($Testing) { write-host "Would:  " } else {
            $params =  @{id_num= $($r.id_num);  newbox=''; newcombo=''}
            $result = invoke-sqlquery -database $ERPDatabase -sqlParameters $params -query $insertUpdateQuery -as PSObjectArray
        }
        write-host "$insertUpdateQuery  `n where:`n $($params | convertTo-Json)"
        $action = "Removed box for $($r.id_num) box was: $($existingDetails.boxno)   $result.result"
    }
    else {
        write-host "Update box to '$($r.newbox)' for id $($r.id_num)"
        if ($r.newBox -notmatch '^\d*$') {
            $err = "box is not a number, or is not 'remove'.  What did you want to do?"
        }
        elseif (  (count($acctDetails)) -eq 0) {
            $err = "person ID number was not found."
        }
        elseif (  ($r.newBox -match '^\d*$')  -and  ( ([int]($r.newbox)) -lt 6000 -or ([int]($r.newbox)) -gt 9000) ){
            $err = "box number $($r.newbox) is out of range - (6000 to 9000)"
        }
        elseif ($r.newcombo -and $r.newcombo -notmatch '^\d+\,\d+\,\d+$') {
            $err = "box number $($r.newbox) combo $($r.newcombo) is not valid - it should be  1,2,3 format  "
        }
        if ($r.newcombo -like '') {
            # Combo not provided on update form.  Get combo out of existing DB
            $combo_r = invoke-sqlquery -database 'tmseprd' -sqlparameters $r -as PSObjectArray -query "
                select combo from _wwn_boxes_and_combos where cpo = @newbox
            "
            if ($combo_r) {$r.newcombo = $combo_r.combo}   # set newcombo for update query below.
        }
        else {
            # update / insert combo in _wwn_box_combos table (TMSEprd)
            $upsert_combo_query = "if exists (select * from _wwn_box_combos where boxno = @newBox)
                BEGIN
                    UPDATE _wwn_box_combos set combo = @newcombo where boxno = @newBox
                    select 'UPDATED' as result
                END ELSE BEGIN
                    INSERT INTO [_wwn_box_combos]   (boxno,   combo)
                                            VALUES  (@newBox, @newcombo)
                    select 'INSERTED' as result
                END
                "
                $result =  invoke-sqlquery -database $ERPDatabase -sqlParameters $r -query  $upsert_combo_query -As PSObjectArray
        }
        
        if (!$err) {
            # $q = "update address_master set addr_line_1 = @newbox, udef_10a_1=@newcombo where addr_cde = 'CPO' and id_num = @id_num"
            if ($Testing) {write-host "Would:" } else {
                $result =  invoke-sqlquery -database $ERPDatabase -sqlParameters $r -query  $insertUpdateQuery -As PSObjectArray
            }
            write-host "$insertUpdateQuery `nwhere:`n $($r | convertto-json)"
            $action = "Added box $($r.newbox) with combo  $result.result"
        }
    }
    write-host "Change action made by $($r.submitter_id): $($r.newbox)  $($r.newcombo)"

    # update the cache table.  This is a blocking transaction so that the boxes_and_combox_view will not crash and burn
    $result =  invoke-sqlquery -database $ERPDatabase  "
                BEGIN TRANSACTION
                DELETE FROM _wwn_box_combos WITH (TABLOCK);
                INSERT INTO _wwn_box_combos
                    SELECT   *
                    FROM tmseprd.._wwn_box_combos_view
                COMMIT
                "
    #One of error or action is blank.
    $msg = "$($r.student_id)  $($existingDetails.first_name)   $($existingDetails.last_name)  " + $err + $action
    write-host $msg
    # $r.submitter_id  submitted the form.
    write-host "add-queuedNotification -FormName '$($formRecord.FormName)' -To '--redacted---@warren-wilson.edu'  -Subject `"Box Number Changes`" -Body '$msg'"
    
    $SubmitterEmail = (get-user $($r.submitter_id) -content 'ad').AD_Email
    $SubmitterEmail += ';--redacted---@warren-wilson.edu'
    add-queuedNotification  -FormName $($formRecord.FormName)  -To $SubmitterEmail -Subject "Box Number Changes" -Body $msg
    $r  # returning someting is necessary (or false on failure) 
}


<########################################


Notification comes from the DB table ....

SELECT TOP (1000) [id]
      ,[formName]
      ,a.[id_num]
      ,[approverType]
      ,[extraEmail]
      ,[status]
	  , nm.first_name, nm.last_name
  FROM [TmsEPrd].[dbo].[_wwn_myWWC_Forms_approvers] a
  join name_master nm on a.id_num = nm.id_num


########################################>

function Notify-Form {
    param(
        [Parameter(Position=0,mandatory=$true)] [string]  $formName,
        [Parameter(Position=1,mandatory=$true)] [string]  $Message,
        [Parameter(Position,mandatory=$false)]  [string]  $To,         # Can be empty.  Will send messages TO the approvertypes passed in. 
        [Parameter(Position,mandatory=$false)]  [string]  $Subject,
        [Parameter(Position,mandatory=$false)]  [string[]] $ApproverTypes = ('manager', 'approver','cc'),
        [Parameter(Position,mandatory=$false)]  [switch]  $Queue
        
    )
    if (!$Subject) {$Subject = "Processed:  $formName"}

    # Pull recipients out of the approver table based on the approverType
    
    $ApproverTypeSet = "('" +  ($approverTypes -replace "'","''" -join "', '") + "')"

    if ($Testing -or $Verbose) { write-host "Query approvers for for $formName '$ApproverTypeSet'"}
    # Get all possible email addresses - from the id num or te extraEmail or CC to one or both or neither etc.
    $ccRecipientsR = invoke-sqlquery  -database $ERPDatabase -sqlParameters @{form=$formName} -query  "
        select concat(string_agg(addr_line_1,';'), ';', string_agg(extraEmail,';')) as cc from _wwn_myWWC_forms_approvers a
        left join address_master am on am.id_num = a.id_num and am.addr_cde = '*eml'
        where formName = @form 
        and approverType in $ApproverTypeSet"
    # And clean it up. Only one email to any duplicates, and trim them out.
    $CC = ''
    foreach ($emailaddr in $ccRecipientsR.cc -split ';') {
        $emailaddr = $emailaddr.trim()
        if ($emailaddr -and $emailaddr -notin $CC) { $CC = $CC + $(if ($CC) {';'} else { ''}) + $emailaddr }
    }
    
    if (!$To) {
        $To = $CC
        $CC = ''
    }
    $To = $To -split ';'
    $cc = $CC -split ';' 

    write-host "Send $formName message '$Subject' to $To  #$($To.count)  cc $CC  #$($cc.count)"
    
    # Testing Mode override delivery
    $To = $(if ($Testing) { $TestingTo } else { $To })
    $cc = $(if ($Testing) { '' } else { $cc })
    if ($To) {
        if ($Queue) {
            write-host "notify-form Queue To: $($to -join '; ') cc: $($cc -join '; ') subject $subject"
            add-QueuedNotification -FormName $formName  -To $To -CC $CC -Subject $Subject -Body $Message 
        }
        else {
            try {
                write-host "notify-form Send To: $($to -join '; ') cc: $($cc -join '; ') subject $subject"
                send-gmailMessage -To $To -CC $CC -Subject $Subject -Body $Message
                write-host "notify-form Sent."
            }
            catch {  # don't let parameter problems in the email cause other things not to go through.
                write-host "Exception:  $( $Error[0].Exception.GetType().FullName )"
                write-host "ERROR in process forms!   Notify Form.  $_"
                write-host $FormName
                write-host [string] $_.ScriptStackTrace
                send-gmailMessage -To '--redacted---+critical@warren-wilson.edu' -Subject 'ERROR IN Process FORMS!' -Body "
                `nNotify-form error. Form Name:   $FormName`n
                `nException:  $( $Error[0].Exception.GetType().FullName) `n
                `nTrace: `n$([string] $_.ScriptStackTrace )

                To:  $($To -join '; ')
                CC:  $($CC -join '; ')
                Subject:  $Subject
                Message:  `n`n$Message`n
                "
            }
        }
    }
}


$queuedNotifications = @{}
function add-QueuedNotification {
    param(
        [Parameter(Position=0,mandatory=$true)]  $FormName,
        [Parameter(Position=1,mandatory=$true)]  $to,
        [Parameter(Position=2,mandatory=$false)] $cc,
        [Parameter(Position=3,mandatory=$true)]  $subject,
        [Parameter(Position=4,mandatory=$true)]  $body
    )

    $n = @{
        to = $to  # Amalgamate To and CC
        cc = $cc
        subject = $subject
        formName = $FormName
        body = $body
    }
    <# for debugsz
    write-host "
    `$queuedNotifications = @{}
    `$n = @{
        to = $to  # Amalgamate To and CC
        cc = $cc
        subject = $subject
        formName = $FormName
        body = $body
    }
    "#>
    
    if ("$FormName|$subject" -notin $queuedNotifications.keys ) {
        $queuedNotifications["$FormName|$subject"] = New-Object System.Collections.Generic.List[System.Object]
    }
    $queuedNotifications["$FormName|$subject"].add($n)
    if ($Testing) { write-host "Queued notification '$FormName|$subject'"}
}

function send-QueuedNotifications{
    foreach ($n in $queuedNotifications.Keys) {
        # Amalgamate To and CC
        $To = @();  foreach ($t in $queuedNotifications[$n].to) { if ($t -notin $To ) { $To += $t } }
        $cc = @();  foreach ($t in $queuedNotifications[$n].cc) { if ($t -notin $cc ) { $cc += $t } }
        $parts = $n.split("|")
        $FormName = $parts[0]
        $Subject = $parts[1]
        #$Subject = $queuedNotifications[$n].subject
        #if ($Subject.count -gt 1) {$Subject = $subject[0]}  # they all have teh same subject and form name
        ##$FormName   = $queuedNotifications[$n].formName
        if ($FormName.count -gt 1) {$FormName = $formName[0]}  # they all have teh same subject and form
        if($Testing) { write-host "Send queued notification '$n'" }
        $Message = "Queued Notifications for $FormName`n`n" + $queuedNotifications[$n].body -join "`n"
        write-host "send-QueuedNotifications -To '$($To -join ';')'' -CC '$($CC -join ';')' -Subject '$Subject' "
        send-gmailMessage -To $($To -join ';') -CC $($CC -join ';') -Subject $Subject -Body $Message
    }
}

function process-form {
    param(
        [Parameter(Position=0,mandatory=$true)] $formRecord
    )

    # call the process function based on the name of the form (with spaces removed)
    # These functions need to return some kind of information when they succeed.  Or nothing (falsey) if they fail.
    # Do a little validation

    $err = ""
    write-host "process-form $($formRecord.formName) for $($formRecord.id_num)  submissionID: $($formRecord.submissionID) submitted: $($formRecord.submitDate)"

    if (!$formRecord.submissionID ) {  $err += "submission ID missing; "  }
    if (!$formRecord.formName )     {  $err += "formName missing; "  }
    if (!$formRecord.submitDate)    {  $err += "submit date missing;"}
    if (!$formRecord.id_num)        {  $err += 'Submitter id_num missing'}
    if ('tmseprd;tmseply;tmsedev' -notlike  "*$($formRecord.dbName.trim())*")  { $err += 'DB name wrong'}

    if (!$err) {
        $funcName = 'process-' + ($formRecord.formName -replace ' ','')

        try {
            $result = (&($funcName) $formRecord)
        }
        catch [System.Management.Automation.CommandNotFoundException] {
            #  if you have recently had an error, inspect $Error[0].Exception.GetType().FullName for name of exception
            write-host "`nError:   Function Not Implemented:  '$funcName' `n  Execption:  $($Error[0].Exception.GetType().FullName)"
        }

        if ($result) {
            if ($Testing) {
                write-host "Would: update myWWC_Forms set processedDate = GETDATE() where id = @id submitDate = @dte // @{id=$($formRecord.submissionID)}  dte=$($formRecord.submitDate)"
            }
            else {
                # note - use of id is deliberate - it's the row ID.  Not id_num:
                $null = invoke-sqlquery -database 'wwn_mgmt' -sqlParameters @{id=$formRecord.submissionID;dte=$formRecord.submitDate} -query "
                    update myWWC_Forms set processedDate = GETDATE() where submissionID = @id and submitDate = @dte"
            }
        }
    }
    else {
        $errMsg = "

        Form needs fixing: $err

        dbName:       $($formrecord.dbName)
        formName:     $($formrecord.formName)
        id_num:       $($formrecord.id_num)
        jics_login:   $($formrecord.jics_login)
        submissionID: $($formrecord.submissionID)
        submitDate:   $($formrecord.SubmitDate)

        "
        write-host "ERROR:   $errMsg"
        send-gmailmessage -to '--redacted---@warren-wilson.edu' -subject "Bad wwc-forms mapped statement" -Body  $ErrMsg
    }
}

function get-unprocessedForms{
    param(
        [Parameter(Position=0)] $forms = '%'
    )
    invoke-sqlquery -database 'wwn_mgmt' -sqlParameters @{name = $forms} -As PSObjectArray -query "
        select  
        campusName = nm.first_Name,
        lastName   = nm.last_Name,
        email      = eml.addr_line_1,
        myWWC_Forms.*
        from [wwn_mgmt].dbo.myWWC_Forms 
        left join [$ERPDatabase].dbo.name_master nm on nm.id_num = myWWC_forms.id_num
        left join [$ERPDatabase].dbo.address_master eml on eml.id_num = myWWC_forms.id_num and addr_cde = '*EML'
        where processedDate is NULL order by submitDate
        "
}


function go {
    # just to mark where the GO is.
}


if ($Go) {
    $todo = get-unprocessedForms
    write-host "wwc_forms $(count($todo)) submissions to process."
    if (count($todo) -gt 0) {
        start-transcript "$ENV:ScriptsRoot\tasks-scheduled\logs\wwc-forms.log" -Append
        
        foreach ($f in $todo) {
            $errors = ""
            try {
                process-form $f 
            }
            catch {
                $formInfo = "Form Name: $($f.formName) SubmissionID: $($f.submissionID)"
                write-host "ERROR in process forms!  $_"
                write-host $formInfo
                write-host [string] $_.ScriptStackTrace  
                $errors = $_ 
                send-gmailMessage -To '--redacted---+critical@warren-wilson.edu' -Subject 'ERROR IN PROCESS FORMS!' -Body "
                Error:   $formInfo`nException:  $( $Error[0].Exception.GetType().FullName) `n Trace: $([string] $_.ScriptStackTrace )"
            }
            
            $monitor = @(
                "$($ENV:Scriptsroot)\include\util\monitor.py"
                "wwc_forms"
                $f.formName
                $errors   # Build Details.  One or two lines.
                $( if ($errors) { "Error"} else {''})   # Short Error Code.  Empty if no error.
                )
            python @monitor
            #$details = $errors                             
            #$err = $( if ($errors) { "Error"} else {''})   # Short Error Code.  Empty if no error.
            #python @monitor $details $err 
        }
        send-queuedNotifications
        stop-transcript
    
    }
}
else {
    write-host "
    Dude.  WWC-FORMS!
    `$ERPDatabase  = '$ERPDatabase'
    `$Testing = `$$Testing
    `$todo = get-unprocessedForms -forms '%'
    foreach (`$f in `$todo) { process-form `$f }

    "
}



