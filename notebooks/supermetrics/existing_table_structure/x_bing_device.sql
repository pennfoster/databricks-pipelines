SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T012_BingDevice](
        [C012_QueryName] [varchar](50) NULL,
        [C012_Date] [date] NULL,
        [C012_Year] [varchar](255) NULL,
        [C012_YearMonth] [varchar](255) NULL,
        [C012_Month] [varchar](255) NULL,
        [C012_DayofMonth] [varchar](255) NULL,
        [C012_DayofWeek] [varchar](255) NULL,
        [C012_DeviceType] [varchar](255) NULL,
        [C012_Keyword] [varchar](255) NULL,
        [C012_KeywordID] [varchar](255) NULL,
        [C012_AdID] [varchar](255) NULL,
        [C012_AdGroupName] [varchar](255) NULL,
        [C012_AdGroupID] [varchar](255) NULL,
        [C012_CampaignName] [varchar](255) NULL,
        [C012_CampaignID] [varchar](255) NULL,
        [C012_AccountName] [varchar](255) NULL,
        [C012_AccountID] [varchar](255) NULL,
        [C012_AccountNumber] [varchar](255) NULL,
        [C012_STATUS] [varchar](255) NULL,
        [C012_CampaignType] [varchar](255) NULL,
        [C012_AdGroupStatus] [varchar](255) NULL,
        [C012_AdType] [varchar](255) NULL,
        [C012_AdStatus] [varchar](255) NULL,
        [C012_Impressions] [float] NULL,
        [C012_Clicks] [float] NULL,
        [C012_Cost] [float] NULL,
        [C012_CtrPercent] [float] NULL,
        [C012_Cpc] [float] NULL,
        [C012_Cpm] [float] NULL,
        [C012_AveragePosition] [float] NULL,
        [C012_Conversions] [float] NULL,
        [C012_ConversionRate] [varchar](255) NULL,
        [C012_ConversionsPerImpressionRate] [varchar](255) NULL,
        [C012_CostPerConversion] [float] NULL,
        [C012_RevenuePerConversion] [float] NULL,
        [C012_Revenue] [float] NULL,
        [C012_ReturnOnAdSpend] [float] NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL
    ) ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T012_BingDevice]
ADD
    DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO