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
package com.iemr.common.identity.repo;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.List;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.iemr.common.identity.domain.MBeneficiaryaddress;

@Repository
public interface BenAddressRepo extends CrudRepository<MBeneficiaryaddress, BigInteger> {

    List<MBeneficiaryaddress> findByBenAddressIDOrderByBenAddressIDAsc(BigInteger benAddressID);

    List<MBeneficiaryaddress> findByCurrPinCodeOrderByBenAddressIDAsc(String currPinCode);

    List<MBeneficiaryaddress> findByPermPinCodeOrderByBenAddressIDAsc(String permPinCode);

    List<MBeneficiaryaddress> findByCurrStateAndCurrDistrictOrderByBenAddressIDAsc(String currState, String currDist);

    List<MBeneficiaryaddress> findByCurrStateIdAndCurrDistrictIdOrderByBenAddressIDAsc(Integer currStateID,
            Integer currDistID);

    List<MBeneficiaryaddress> findByPermStateAndPermDistrictOrderByBenAddressIDAsc(String permState, String permDist);

    List<MBeneficiaryaddress> findByPermStateIdAndPermDistrictIdOrderByBenAddressIDAsc(Integer permStateID,
            Integer permDistID);

    List<MBeneficiaryaddress> findByEmerStateAndEmerDistrictOrderByBenAddressIDAsc(String emerState, String emerDist);

    List<MBeneficiaryaddress> findByEmerStateIdAndEmerDistrictIdOrderByBenAddressIDAsc(Integer emerStateID,
            Integer emerDistID);

    List<MBeneficiaryaddress> findByCreatedDateBetweenOrderByBenAddressIDAsc(Timestamp fromDate, Timestamp toDate);

    @Query("select a from MBeneficiaryaddress a where a.currAddressValue = :address or "
            + "a.emerAddressValue = :address or a.permAddressValue = :address order by a.benAddressID asc")
    List<MBeneficiaryaddress> findByAddress(String address);

    @Transactional
    @Modifying
    @Query(" UPDATE MBeneficiaryaddress set vanSerialNo = :benAddressID WHERE benAddressID = :benAddressID")
    int updateVanSerialNo(@Param("benAddressID") BigInteger benAddressID);

    @Query("SELECT benAddressID FROM MBeneficiaryaddress WHERE vanSerialNo =:vanSerialNo AND vanID =:vanID ")
    BigInteger findIdByVanSerialNoAndVanID(@Param("vanSerialNo") BigInteger vanSerialNo, @Param("vanID") Integer vanID);

    @Query("SELECT a FROM MBeneficiaryaddress a WHERE a.vanSerialNo =:vanSerialNo AND a.vanID =:vanID ")
    MBeneficiaryaddress getWithVanSerialNoVanID(@Param("vanSerialNo") BigInteger vanSerialNo,
            @Param("vanID") Integer vanID);

    /**
     * Get user location details from m_userservicerolemapping
     */
    @Query(value = "SELECT "
            + "ProviderServiceMapID, " +
            "Blockid, " + 
            "Villageid, " + 
            "WorkingLocationID " +
            "FROM db_iemr.m_userservicerolemapping "
            + "WHERE UserID = :userId "
            + "AND Deleted = false "
            + "LIMIT 1",
            nativeQuery = true)
    List<Object[]> getUserLocation(@Param("userId") Integer userId);

}
