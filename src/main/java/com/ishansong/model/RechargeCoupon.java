package com.ishansong.model;

import lombok.Data;

import java.util.Date;

@Data
public class RechargeCoupon {
    long activity_group_id;
    String activity_group_name;
    long activity_id;
    String activity_name;
    long activity_status;
    long city_id;
    String city_name;
    long coupon_able_cnt;
    long coupon_amt;
    long coupon_discount_amt;
    long coupon_exp_cnt;
    long coupon_finish_amt;
    long coupon_finish_cnt;
    String coupon_type;
    String coupon_type_name;
    long coupon_user_cnt;
    String dt_ymd;
    String end_time;
    String start_time;
    String etl_time;
    long expire_days;
    long per_assign_count;
    long per_recharge_amt;
    long rev_cnt;
    long rev_coupon_cnt;
    String unid;
}
