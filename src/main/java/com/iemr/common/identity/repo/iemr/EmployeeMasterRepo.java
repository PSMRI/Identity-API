package com.iemr.common.identity.repo.iemr;

import com.iemr.common.identity.domain.iemr.M_User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EmployeeMasterRepo extends JpaRepository<M_User,Integer> {
    M_User findByUserID(Integer userID);

    M_User getUserByUserID(Integer parseLong);
}