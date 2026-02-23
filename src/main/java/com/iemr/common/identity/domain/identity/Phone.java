/*
* AMRIT – Accessible Medical Records via Integrated Technology 
* Integrated EHR (Electronic Health Records) Solution 
*
* Copyright (C) "Piramal Swasthya Management and Research Institute" 
*
* This file is part of AMRIT.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program.  If not, see https://www.gnu.org/licenses/.
*/
package com.iemr.common.identity.domain.identity;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class Phone
{
	private String phoneNum;
	private String phoneType;
	private Boolean isPreferredCallIncoming;
	private Boolean isPreferredCallOutgoing;
	private Boolean isPreferredForSMSSend;
	private Boolean isPreferredForSMSRecv;
	private Boolean isSmartPhone;
	private Boolean isSelfNumber;
	private String belongsToBenRegId;
	private String belongsToName;

	public static List<Phone> createContactList(MBeneficiarycontact benContact, String belongsToBenRegId,
			String belongsToName)
	{
		List<Phone> contactsList = null;
		if (benContact.getPreferredPhoneNum() != null || benContact.getPhoneNum1() != null
				|| benContact.getPhoneNum2() != null || benContact.getPhoneNum3() != null
				|| benContact.getPhoneNum4() != null || benContact.getPhoneNum5() != null)
		{
			contactsList = new ArrayList<Phone>();
			Phone phoneMap = null;
			if (benContact.getPreferredPhoneNum() != null)
			{
				phoneMap = new Phone();
				phoneMap.setPhoneNum(benContact.getPreferredPhoneNum());
				phoneMap.setPhoneType(benContact.getPreferredPhoneTyp());
				phoneMap.setBelongsToBenRegId(belongsToBenRegId);
				phoneMap.setBelongsToName(belongsToName);
				contactsList.add(phoneMap);
			}
			if (benContact.getPhoneNum1() != null)
			{
				phoneMap = new Phone();
				phoneMap.setPhoneNum(benContact.getPhoneNum1());
				phoneMap.setPhoneType(benContact.getPhoneTyp1());
				phoneMap.setBelongsToBenRegId(belongsToBenRegId);
				phoneMap.setBelongsToName(belongsToName);
				contactsList.add(phoneMap);
			}
			if (benContact.getPhoneNum2() != null)
			{
				phoneMap = new Phone();
				phoneMap.setPhoneNum(benContact.getPhoneNum2());
				phoneMap.setPhoneType(benContact.getPhoneTyp2());
				phoneMap.setBelongsToBenRegId(belongsToBenRegId);
				phoneMap.setBelongsToName(belongsToName);
				contactsList.add(phoneMap);
			}
			if (benContact.getPhoneNum3() != null)
			{
				phoneMap = new Phone();
				phoneMap.setPhoneNum(benContact.getPhoneNum3());
				phoneMap.setPhoneType(benContact.getPhoneTyp3());
				phoneMap.setBelongsToBenRegId(belongsToBenRegId);
				phoneMap.setBelongsToName(belongsToName);
				contactsList.add(phoneMap);
			}
			if (benContact.getPhoneNum4() != null)
			{
				phoneMap = new Phone();
				phoneMap.setPhoneNum(benContact.getPhoneNum4());
				phoneMap.setPhoneType(benContact.getPhoneTyp4());
				phoneMap.setBelongsToBenRegId(belongsToBenRegId);
				phoneMap.setBelongsToName(belongsToName);
				contactsList.add(phoneMap);
			}
			if (benContact.getPhoneNum5() != null)
			{
				phoneMap = new Phone();
				phoneMap.setPhoneNum(benContact.getPhoneNum5());
				phoneMap.setPhoneType(benContact.getPhoneTyp5());
				phoneMap.setBelongsToBenRegId(belongsToBenRegId);
				phoneMap.setBelongsToName(belongsToName);
				contactsList.add(phoneMap);
			}
		}
		return contactsList;
	}
}
