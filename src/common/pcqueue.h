/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <inttypes.h>

void* queue_new(uint32_t size);
void queue_delete(void *que);
int queue_isempty(void *que);
uint32_t queue_elements(void *que);
int queue_isfull(void *que);
uint32_t queue_sizeleft(void *que);
void queue_put(void *que,uint32_t id,uint32_t op,uint8_t *data,uint32_t leng);
int queue_tryput(void *que,uint32_t id,uint32_t op,uint8_t *data,uint32_t leng);
void queue_get(void *que,uint32_t *id,uint32_t *op,uint8_t **data,uint32_t *leng);
int queue_tryget(void *que,uint32_t *id,uint32_t *op,uint8_t **data,uint32_t *leng);
