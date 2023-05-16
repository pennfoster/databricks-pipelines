SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T003_BingKeyword](
        [C003_QueryName] [varchar](50) NULL,
        [C003_Date] [date] NULL,
        [C003_Keyword] [varchar](255) NULL,
        [C003_CustomURLParameters] [varchar](max) NULL,
        [C003_Year] [varchar](255) NULL,
        [C003_YearMonth] [varchar](255) NULL,
        [C003_Month] [varchar](255) NULL,
        [C003_DayofMonth] [varchar](255) NULL,
        [C003_DayofWeek] [varchar](255) NULL,
        [C003_AccountName] [varchar](255) NULL,
        [C003_AccountID] [varchar](255) NULL,
        [C003_AccountNumber] [varchar](255) NULL,
        [C003_CampaignName] [varchar](255) NULL,
        [C003_CampaignID] [varchar](255) NULL,
        [C003_STATUS] [varchar](255) NULL,
        [C003_AdGroupName] [varchar](255) NULL,
        [C003_AdGroupID] [varchar](255) NULL,
        [C003_AdGroupStatus] [varchar](255) NULL,
        [C003_FinalURL] [varchar](max) NULL,
        [C003_FinalMobileURL] [varchar](max) NULL,
        [C003_DeliveredMatchType] [varchar](255) NULL,
        [C003_KeywordID] [varchar](255) NULL,
        [C003_KeywordStatus] [varchar](255) NULL,
        [C003_CurrentMaxCPC] [decimal](9, 2) NULL,
        [C003_CampaignLabels] [varchar](255) NULL,
        [C003_AdGroupLabels] [varchar](255) NULL,
        [C003_KeywordLabels] [varchar](255) NULL,
        [C003_Impressions] [int] NULL,
        [C003_Clicks] [int] NULL,
        [C003_Cost] [decimal](9, 2) NULL,
        [C003_CTRPercent] [float] NULL,
        [C003_CPC] [decimal](9, 2) NULL,
        [C003_CPM] [decimal](9, 2) NULL,
        [C003_AveragePosition] [float] NULL,
        [C003_QualityScore] [int] NULL,
        [C003_Conversions] [int] NULL,
        [C003_ConversionRate] [varchar](255) NULL,
        [C003_ConversionsPerImpressionRate] [varchar](255) NULL,
        [C003_CostPerConversion] [decimal](9, 2) NULL,
        [C003_RevenuePerConversion] [decimal](9, 2) NULL,
        [C003_Revenue] [decimal](9, 2) NULL,
        [C003_ReturnOnAdSpend] [float] NULL,
        [C003_Mkwid] [varchar](255) NULL,
        [C003_Pubcode] [varchar](255) NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL,
        [C003_Adkey] [varchar](255) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T003_BingKeyword]
ADD
    DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO