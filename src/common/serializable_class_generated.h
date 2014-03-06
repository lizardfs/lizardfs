#pragma once

// DO NOT MODIFY THIS FILE BY HAND!
//
// This file was automatically generated with serializable_class_generate.sh. If you want macros below to
// support more then 15 parameters, generate this file once again.

#define APPLY1_1(Macro, Sep, t1) Macro(t1)
#define APPLY1_2(Macro, Sep, t1, t2) Macro(t1) Sep() APPLY1_1(Macro, Sep, t2)
#define APPLY1_3(Macro, Sep, t1, t2, t3) Macro(t1) Sep() APPLY1_2(Macro, Sep, t2, t3)
#define APPLY1_4(Macro, Sep, t1, t2, t3, t4) Macro(t1) Sep() APPLY1_3(Macro, Sep, t2, t3, t4)
#define APPLY1_5(Macro, Sep, t1, t2, t3, t4, t5) Macro(t1) Sep() APPLY1_4(Macro, Sep, t2, t3, t4, t5)
#define APPLY1_6(Macro, Sep, t1, t2, t3, t4, t5, t6) Macro(t1) Sep() APPLY1_5(Macro, Sep, t2, t3, t4, t5, t6)
#define APPLY1_7(Macro, Sep, t1, t2, t3, t4, t5, t6, t7) Macro(t1) Sep() APPLY1_6(Macro, Sep, t2, t3, t4, t5, t6, t7)
#define APPLY1_8(Macro, Sep, t1, t2, t3, t4, t5, t6, t7, t8) Macro(t1) Sep() APPLY1_7(Macro, Sep, t2, t3, t4, t5, t6, t7, t8)
#define APPLY1_9(Macro, Sep, t1, t2, t3, t4, t5, t6, t7, t8, t9) Macro(t1) Sep() APPLY1_8(Macro, Sep, t2, t3, t4, t5, t6, t7, t8, t9)
#define APPLY1_10(Macro, Sep, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10) Macro(t1) Sep() APPLY1_9(Macro, Sep, t2, t3, t4, t5, t6, t7, t8, t9, t10)
#define APPLY1_11(Macro, Sep, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11) Macro(t1) Sep() APPLY1_10(Macro, Sep, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11)
#define APPLY1_12(Macro, Sep, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12) Macro(t1) Sep() APPLY1_11(Macro, Sep, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12)
#define APPLY1_13(Macro, Sep, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13) Macro(t1) Sep() APPLY1_12(Macro, Sep, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13)
#define APPLY1_14(Macro, Sep, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14) Macro(t1) Sep() APPLY1_13(Macro, Sep, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14)
#define APPLY1_15(Macro, Sep, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15) Macro(t1) Sep() APPLY1_14(Macro, Sep, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15)

#define APPLY2_2(Macro, Sep, T1, t1) Macro(T1, t1)
#define APPLY2_4(Macro, Sep, T1, t1, T2, t2) Macro(T1, t1) Sep() APPLY2_2(Macro, Sep, T2, t2)
#define APPLY2_6(Macro, Sep, T1, t1, T2, t2, T3, t3) Macro(T1, t1) Sep() APPLY2_4(Macro, Sep, T2, t2, T3, t3)
#define APPLY2_8(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4) Macro(T1, t1) Sep() APPLY2_6(Macro, Sep, T2, t2, T3, t3, T4, t4)
#define APPLY2_10(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4, T5, t5) Macro(T1, t1) Sep() APPLY2_8(Macro, Sep, T2, t2, T3, t3, T4, t4, T5, t5)
#define APPLY2_12(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6) Macro(T1, t1) Sep() APPLY2_10(Macro, Sep, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6)
#define APPLY2_14(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7) Macro(T1, t1) Sep() APPLY2_12(Macro, Sep, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7)
#define APPLY2_16(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8) Macro(T1, t1) Sep() APPLY2_14(Macro, Sep, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8)
#define APPLY2_18(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9) Macro(T1, t1) Sep() APPLY2_16(Macro, Sep, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9)
#define APPLY2_20(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10) Macro(T1, t1) Sep() APPLY2_18(Macro, Sep, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10)
#define APPLY2_22(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11) Macro(T1, t1) Sep() APPLY2_20(Macro, Sep, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11)
#define APPLY2_24(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12) Macro(T1, t1) Sep() APPLY2_22(Macro, Sep, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12)
#define APPLY2_26(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12, T13, t13) Macro(T1, t1) Sep() APPLY2_24(Macro, Sep, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12, T13, t13)
#define APPLY2_28(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12, T13, t13, T14, t14) Macro(T1, t1) Sep() APPLY2_26(Macro, Sep, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12, T13, t13, T14, t14)
#define APPLY2_30(Macro, Sep, T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12, T13, t13, T14, t14, T15, t15) Macro(T1, t1) Sep() APPLY2_28(Macro, Sep, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12, T13, t13, T14, t14, T15, t15)

#define VARS_COMMAS_2(T1, t1) t1
#define VARS_COMMAS_4(T1, t1, T2, t2) t1, t2
#define VARS_COMMAS_6(T1, t1, T2, t2, T3, t3) t1, t2, t3
#define VARS_COMMAS_8(T1, t1, T2, t2, T3, t3, T4, t4) t1, t2, t3, t4
#define VARS_COMMAS_10(T1, t1, T2, t2, T3, t3, T4, t4, T5, t5) t1, t2, t3, t4, t5
#define VARS_COMMAS_12(T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6) t1, t2, t3, t4, t5, t6
#define VARS_COMMAS_14(T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7) t1, t2, t3, t4, t5, t6, t7
#define VARS_COMMAS_16(T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8) t1, t2, t3, t4, t5, t6, t7, t8
#define VARS_COMMAS_18(T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9) t1, t2, t3, t4, t5, t6, t7, t8, t9
#define VARS_COMMAS_20(T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10) t1, t2, t3, t4, t5, t6, t7, t8, t9, t10
#define VARS_COMMAS_22(T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11) t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11
#define VARS_COMMAS_24(T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12) t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12
#define VARS_COMMAS_26(T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12, T13, t13) t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13
#define VARS_COMMAS_28(T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12, T13, t13, T14, t14) t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14
#define VARS_COMMAS_30(T1, t1, T2, t2, T3, t3, T4, t4, T5, t5, T6, t6, T7, t7, T8, t8, T9, t9, T10, t10, T11, t11, T12, t12, T13, t13, T14, t14, T15, t15) t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15

// File generated correctly.
