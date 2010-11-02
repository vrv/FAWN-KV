/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _PRINT_H_
#define _PRINT_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <iostream>
#include <iomanip>
#include <list>
#include <vector>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sstream>

using namespace std;

void bytes_into_hex_string(const u_char *data, u_int len, string &dststr);
string bytes_to_hex(const u_char* data, u_int len);
string bytes_to_hex(const string& s);
int get_digit_value(char digit);
string* hex_string_to_bytes(const u_char* hex_num, u_int len);
void print_hex_ascii_line(const u_char *payload, u_int len, u_int offset);
void print_payload(const u_char *payload, u_int len);
void print_payload(const string &str);
void tokenize(const string& str, vector<string>& tokens, const string& delimiters);

void int_to_byte(const uint32_t i, char* p_byte_value);
int fill_file_with_zeros(int fd, size_t nbytes);


string getID(string ip, const int32_t port);
string getIP(string id);
int getPort(string id);

template <class T>
inline string to_string(const T& t)
{
  stringstream ss;
  ss << t;
  return ss.str();
}

#endif //_PRINT_H_
