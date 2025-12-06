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

import com.iemr.common.identity.domain.MBeneficiarymapping;
import com.iemr.common.identity.domain.MBeneficiaryservicemapping;
import com.iemr.common.identity.domain.VBenAdvanceSearch;
import com.iemr.common.identity.dto.IdentityDTO;
import com.iemr.common.identity.dto.IdentitySearchDTO;

@Repository
public interface BenMappingRepo extends CrudRepository<MBeneficiarymapping, BigInteger>, BenMappingRepoCustom {
	MBeneficiarymapping findByBenMapIdOrderByBenMapIdAsc(BigInteger benMapId);

	MBeneficiarymapping findByBenRegIdOrderByBenMapIdAsc(BigInteger benRegId);

	@Query("select m from MBeneficiarymapping m where m.mBeneficiarydetail.beneficiaryDetailsId = :beneficiaryDetailsId"
			+ " order by m.benMapId Asc")
	MBeneficiarymapping findByBeneficiaryDetailsId(@Param("beneficiaryDetailsId") BigInteger beneficiaryDetailsId);

	@Query(value = "select m from MBeneficiarymapping m where (m.mBeneficiarycontact.preferredPhoneNum = :phoneNum "
			+ "or m.mBeneficiarycontact.phoneNum1 = :phoneNum or m.mBeneficiarycontact.phoneNum2 = :phoneNum "
			+ "or m.mBeneficiarycontact.phoneNum3 = :phoneNum or m.mBeneficiarycontact.phoneNum4 = :phoneNum "
			+ "or m.mBeneficiarycontact.phoneNum5 = :phoneNum) order by m.benMapId Desc")
	List<MBeneficiarymapping> findByBeneficiaryDetailsByPhoneNumber(@Param("phoneNum") String phoneNum);

	@Query("select m from MBeneficiarymapping m where m.benRegId in :benRegIds order by m.benMapId Asc")
	List<MBeneficiarymapping> findAllByBenRegIdOrderByBenMapIdAsc(@Param("benRegIds") List<BigInteger> benRegIds);

	@Query("select t from MBeneficiarymapping t where t.benRegId = :benRegID")
	MBeneficiarymapping getBenImageIdByBenRegID(@Param("benRegID") BigInteger benRegID);

	@Transactional
	@Modifying
	@Query(" UPDATE MBeneficiarymapping set vanSerialNo = :benMapId WHERE benMapId = :benMapId")
	int updateVanSerialNo(@Param("benMapId") BigInteger benMapId);

	@Query("SELECT t.benMapId, t.benAddressId, t.benConsentId, t.benContactsId, t.benDetailsId, "
			+ " t.benRegId, t.benImageId, t.benAccountID, t.vanID, t.vanSerialNo, "
			+ " t.createdBy, t.createdDate FROM MBeneficiarymapping t "
			+ " WHERE t.benRegId = :benRegID ORDER BY t.benMapId Desc ")
	public List<Object[]> getBenMappingByRegID(@Param("benRegID") BigInteger benRegID);

	@Query("SELECT t.benMapId, t.benAddressId, t.benConsentId, t.benContactsId, t.benDetailsId, "
			+ " t.benRegId, t.benImageId, t.benAccountID, t.vanID, t.vanSerialNo, "
			+ " t.createdBy, t.createdDate FROM MBeneficiarymapping t "
			+ "  WHERE t.benContactsId IN :contactIDList  ORDER BY t.benMapId Desc ")
	public List<Object[]> getBenMappingByBenContactIdList(@Param("contactIDList") List<BigInteger> contactIDList);

	@Query("SELECT t.benMapId, t.benAddressId, t.benConsentId, t.benContactsId, t.benDetailsId, "
			+ " t.benRegId, t.benImageId, t.benAccountID, t.vanID, t.vanSerialNo, "
			+ " t.createdBy, t.createdDate FROM MBeneficiarymapping t "
			+ "  WHERE t.benContactsId =:benContactId AND t.vanID = :vanID ")
	public List<Object[]> getBenMappingByBenContactIdListNew(@Param("benContactId") BigInteger benContactId,
			@Param("vanID") Integer vanID);

	@Query(value = "SELECT BenMapId, BenAddressId, BenConsentId, BenContactsId, BenDetailsId, "
			+ " BenRegId, BenImageId, BenAccountID, VanID, VanSerialNo, "
			+ " CreatedBy, CreatedDate FROM i_beneficiarymapping "
			+ " WHERE BenRegId IN :benRegIDList ORDER BY BenMapId Desc ", nativeQuery = true)
	public List<Object[]> getBenMappingByRegIDList(@Param("benRegIDList") List<BigInteger> benRegIDList);

	@Query("SELECT t FROM MBeneficiarymapping t WHERE t.benRegId =:benRegId")
	public MBeneficiarymapping getBenDetailsId(@Param("benRegId") BigInteger benRegId);

	@Query("SELECT t.benRegId FROM MBeneficiarymapping t WHERE t.benDetailsId =:benDetailsId AND vanID = :vanID")
	public BigInteger getBenRegId(@Param("benDetailsId") BigInteger benDetailsId, @Param("vanID") Integer vanID);

	@Query("SELECT t.benMapId, t.benAddressId, t.benConsentId, t.benContactsId, t.benDetailsId, "
			+ " t.benRegId, t.benImageId, t.benAccountID, t.vanID, t.vanSerialNo, "
			+ " t.createdBy, t.createdDate FROM MBeneficiarymapping t "
			+ "  WHERE t.benDetailsId IN :benDetailsIds AND t.vanID = :vanID ")
	public List<Object[]> getBenMappingByBenDetailsIds(@Param("benDetailsIds") List<BigInteger> benDetailsIds,
			@Param("vanID") Integer vanID);

	@Query("SELECT t.benMapId, t.benAddressId, t.benConsentId, t.benContactsId, t.benDetailsId, "
			+ " t.benRegId, t.benImageId, t.benAccountID, t.vanID, t.vanSerialNo, "
			+ " t.createdBy, t.createdDate FROM MBeneficiarymapping t "
			+ "  WHERE t.vanSerialNo =:benMapIds AND t.vanID = :vanID ")
	public List<Object[]> getBenMappingByVanSerialNo(@Param("benMapIds") BigInteger benMapIds,
			@Param("vanID") Integer vanID);

	@Query(value = "select m from MBeneficiarymapping m where m.mBeneficiaryaddress.permVillageId IN :villageIDs and "
			+ "(m.mBeneficiaryaddress.lastModDate > :lastModDate or m.mBeneficiarycontact.lastModDate > :lastModDate "
			+ "or m.mBeneficiarydetail.lastModDate > :lastModDate ) order by m.benMapId Desc")
	List<MBeneficiarymapping> findByBeneficiaryDetailsByVillageIDAndLastModifyDate(
			@Param("villageIDs") List<Integer> villageID, @Param("lastModDate") Timestamp lastModifiedDate);

	@Query(value = "select COUNT(m) from MBeneficiarymapping m where m.mBeneficiaryaddress.permVillageId IN :villageIDs and "
			+ "(m.mBeneficiaryaddress.lastModDate > :lastModDate or m.mBeneficiarycontact.lastModDate > :lastModDate "
			+ "or m.mBeneficiarydetail.lastModDate > :lastModDate ) order by m.benMapId Desc")
	Long getBeneficiaryCountsByVillageIDAndLastModifyDate(@Param("villageIDs") List<Integer> villageID,
			@Param("lastModDate") Timestamp lastModifiedDate);

	
	//@Query("SELECT t FROM MBeneficiarymapping t WHERE t.vanSerialNo =:vanSerialNo AND t.vanID=:vanID")
	@Query(value = "select bm FROM  MBeneficiarymapping bm "
			+"LEFT JOIN MBeneficiaryregidmapping brm ON brm.benRegId=bm.benRegId and brm.vanID=bm.vanID "
			+"LEFT JOIN MBeneficiarycontact bc ON bc.vanSerialNo = bm.benContactsId and bm.vanID=bc.vanID "
			+"LEFT JOIN MBeneficiarydetail bd on bm.benDetailsId = bd.vanSerialNo and bm.vanID=bd.vanID "
			+"LEFT JOIN MBeneficiaryaddress ba ON ba.vanSerialNo = bm.benAddressId and bm.vanID=ba.vanID "
			+"LEFT JOIN MBeneficiaryconsent bt on bt.vanSerialNo=bm.benConsentId and bm.vanID=bt.vanID "
			//+"LEFT JOIN MBensecurestack bs on bs.benSecureStackId=bm.benSecureStackId and bm.vanID=bs.vanID "
			+"LEFT JOIN MBeneficiaryImage bi on bi.vanSerialNo=bm.benImageId and bm.vanID=bi.vanID "
			+"LEFT JOIN MBeneficiaryAccount bac on bac.vanSerialNo=bm.benAccountID and bm.vanID=bac.vanID "
			+"LEFT JOIN MBeneficiaryidentity bid on bid.vanSerialNo=bm.benMapId and bm.vanID=bid.vanID "
			+"LEFT JOIN MBeneficiaryfamilymapping bfm on bfm.vanSerialNo=bm.benMapId and bm.vanID=bfm.vanID "
			+"LEFT JOIN MBeneficiaryservicemapping bsm on bsm.vanSerialNo=bm.benMapId and bm.vanID=bsm.vanID "
			+"where bm.vanSerialNo=:vanSerialNo and bm.vanID=:vanID")
	MBeneficiarymapping getMapping(@Param("vanSerialNo") BigInteger vanSerialNo,@Param("vanID") Integer vanID);
	
	@Query("SELECT a FROM MBeneficiarymapping a WHERE a.vanSerialNo =:vanSerialNo AND a.vanID =:vanID ")
	MBeneficiarymapping getWithVanSerialNoVanID(@Param("vanSerialNo") BigInteger vanSerialNo,
			@Param("vanID") Integer vanID);

	@Query("SELECT b.benRegId, b.benMapId FROM MBeneficiarymapping b where benRegId is not null and deleted = false")
	List<Object[]> getAllBeneficiaryIds();

	/**
     * Get beneficiary IDs in batches for efficient processing
     * @param offset Starting position
     * @param limit Number of records to fetch
     */
    @Query(value = "SELECT benRegId, benMapId FROM MBeneficiarymapping " +
                   "WHERE deleted = false ORDER BY benRegId LIMIT :limit OFFSET :offset")
    List<Object[]> getBeneficiaryIdsBatch(@Param("offset") int offset, @Param("limit") int limit);
    
    /**
     * Count total non-deleted beneficiaries
     */
    @Query(value = "SELECT COUNT(*) FROM MBeneficiarymapping WHERE deleted = false")
    long countActiveBeneficiaries();


      @Query("SELECT m FROM MBeneficiarymapping m " +
           "LEFT JOIN FETCH m.mBeneficiarydetail " +
           "LEFT JOIN FETCH m.mBeneficiarycontact " +
           "LEFT JOIN FETCH m.mBeneficiaryaddress " +
           "WHERE m.benRegId = :benRegId AND m.deleted = false")
    MBeneficiarymapping findByBenRegIdWithDetails(@Param("benRegId") BigInteger benRegId);
    
    /**
     * Simple find by benRegId
     */
    @Query("SELECT m FROM MBeneficiarymapping m WHERE m.benRegId = :benRegId AND m.deleted = false")
    MBeneficiarymapping findByBenRegId(@Param("benRegId") BigInteger benRegId);
    
    /**
     * Check if beneficiary exists by benRegId
     */
    @Query(value = "SELECT COUNT(*) > 0 FROM MBeneficiarymapping WHERE benRegId = :benRegId AND deleted = false")
    boolean existsByBenRegId(@Param("benRegId") BigInteger benRegId);

}
