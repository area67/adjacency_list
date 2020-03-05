#pragma once

class __attribute__((aligned(64))) ThreadData
{
    public:
        int g_commits = 0;
        int g_aborts = 0;
};