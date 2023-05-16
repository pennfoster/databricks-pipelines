SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T007_BingAdGroup](
        [C007_QueryName] [varchar](50) NULL,
        [C007_Date] [date] NULL,
        [C007_Year] [varchar](255) NULL,
        [C007_YearMonth] [varchar](255) NULL,
        [C007_Month] [varchar](255) NULL,
        [C007_DayofMonth] [varchar](255) NULL,
        [C007_DayofWeek] [varchar](255) NULL,
        [C007_AdGroupName] [varchar](255) NULL,
        [C007_AdGroupID] [varchar](255) NULL,
        [C007_AccountName] [varchar](255) NULL,
        [C007_AccountID] [varchar](255) NULL,
        [C007_AccountNumber] [varchar](255) NULL,
        [C007_CampaignName] [varchar](255) NULL,
        [C007_CampaignID] [varchar](255) NULL,
        [C007_STATUS] [varchar](255) NULL,
        [C007_BidStrategyType] [varchar](255) NULL,
        [C007_AdGroupStatus] [varchar](255) NULL,
        [C007_CustomParameters] [varchar](255) NULL,
        [C007_CampaignLabels] [varchar](255) NULL,
        [C007_AdGroupLabels] [varchar](255) NULL,
        [C007_Impressions] [float] NULL,
        [C007_Clicks] [float] NULL,
        [C007_Cost] [float] NULL,
        [C007_CTRPercentage] [float] NULL,
        [C007_CPC] [float] NULL,
        [C007_CPM] [float] NULL,
        [C007_AveragePosition] [float] NULL,
        [C007_Conversions] [float] NULL,
        [C007_ConversionRate] [varchar](255) NULL,
        [C007_ConversionsPerImpressionRate] [varchar](255) NULL,
        [C007_CostPerConversion] [float] NULL,
        [C007_RevenuePerConversion] [float] NULL,
        [C007_Revenue] [float] NULL,
        [C007_ReturnOnAdSpend] [float] NULL,
        [C007_Mkwid] [varchar](255) NULL,
        [C007_Pubcode] [varchar](255) NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL,
        [C007_Adkey] [varchar](255) NULL
    ) ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T007_BingAdGroup]
ADD
    DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO