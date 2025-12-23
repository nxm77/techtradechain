/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

#define SINGLE_PROOF_SIZE 672
#define POINT_SIZE 32

typedef struct {
	void* proof;
	void* commitment;
	void* opening;
} proof_content;

int bulletproofs_generate_random_scalar(void*);

int bulletproofs_prove_with_random_opening(proof_content*, unsigned int);
int bulletproofs_prove_with_specific_opening(proof_content*, unsigned int, void*);
int bulletproofs_verify_single(void*, void*);

int pedersen_commit_with_random_opening(void*, void*, unsigned int);
int pedersen_commit_with_specific_opening(void*, void*, unsigned int);
int pedersen_verify(void*, void*, unsigned int);

int pedersen_point_neg(void*, void*);
int pedersen_point_add(void*, void*, void*);
int pedersen_point_sub(void*, void*, void*);

int pedersen_scalar_neg(void*, void*);
int pedersen_scalar_add(void*, void*, void*);
int pedersen_scalar_sub(void*, void*, void*);
int pedersen_scalar_mul(void*, void*, unsigned int);
int pedersen_scalar_div(void*, void*, unsigned int);

int pedersen_commitment_add_num(void*, void*, unsigned int);
//int pedersen_commitment_add(void*, void*, void*, void*, void*, void*);
int pedersen_commitment_sub_num(void*, void*, unsigned int);
//int pedersen_commitment_sub(void*, void*, void*, void*, void*, void*);
int pedersen_commitment_mul_num(void*, void*, unsigned int);
//int pedersen_commitment_div_num(void*, void*, unsigned int);
